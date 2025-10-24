#!/usr/bin/env python3
import os
import sys
import time
import signal
import traceback
import schedule
import requests
from datetime import datetime, timedelta
from typing import List, Optional
from alpaca_trade_api import REST, TimeFrame
from alpaca_trade_api.rest import APIError

# -------------------------
# CONFIGURATION
# -------------------------
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Risk & trading settings
RISK_PER_TRADE = 0.02
STOP_LOSS_PCT = 0.35
TOP_N_STOCKS = 10
MIN_PRICE = 3
MAX_PRICE = 50
MIN_VOLUME = 1_000_000
MIN_OPTION_VOLUME = 50
MIN_OPTION_PRICE = 0.5
MIN_DAYS_TO_EXPIRY = 3
MAX_RETRIES = 3
RETRY_SLEEP = 3
TRADE_INTERVAL_MINUTES = 30

# Scheduler times
DAILY_REFRESH_TIME = "09:45"
DAILY_TRADE_TIME = "09:50"

# -------------------------
# Alpaca client
# -------------------------
api = REST(API_KEY, API_SECRET, BASE_URL)

# -------------------------
# STATE
# -------------------------
symbols: List[str] = []
purchased_options = set()

# -------------------------
# DISCORD HELPERS
# -------------------------
def send_discord_message(message: str, critical: bool = False) -> None:
    """
    Send a message to Discord via webhook.
    If critical=True it will include a mention of @here (if allowed by channel).
    """
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] No webhook URL set in environment.")
        return
    try:
        payload = {"content": ("@here\n" if critical else "") + message}
        # short timeout to avoid blocking
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=6)
        if resp.status_code not in (200, 204):
            print(f"[DiscordError] status {resp.status_code}: {resp.text}")
    except Exception as e:
        # don't raise from inside a monitoring helper
        print(f"[DiscordError] Exception sending message: {e}")

def send_critical_alert(title: str, exc: Optional[BaseException] = None) -> None:
    """
    Send a critical alert with a stack trace (if exc provided) to Discord.
    """
    try:
        trace = ""
        if exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            trace = f"\n```{tb[-1900:]}```"  # truncate to last ~1900 chars to fit message
        else:
            # capture current stack
            tb = "".join(traceback.format_stack())
            trace = f"\n```{tb[-1900:]}```"
        message = f"🔥 CRITICAL: {title} at {datetime.now():%Y-%m-%d %H:%M:%S}{trace}"
        send_discord_message(message, critical=True)
    except Exception as e:
        print(f"[CriticalAlertError] {e}")

# -------------------------
# SAFE CALL HELPERS
# -------------------------
def safe_api_call(fn, *args, **kwargs):
    """
    Generic retry wrapper around Alpaca calls to reduce transient failures.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"[Retry {attempt}/{MAX_RETRIES}] {fn.__name__} failed: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_SLEEP)

# -------------------------
# MARKET / DATA HELPERS
# -------------------------
def is_market_open() -> bool:
    """Return True if market is currently open."""
    try:
        clock = safe_api_call(api.get_clock)
        return getattr(clock, "is_open", False)
    except Exception as e:
        # If repeated 401 unauthorized occurs, send a critical alert once
        if isinstance(e, Exception) and "401" in str(e):
            send_critical_alert("401 Unauthorized from Alpaca API during get_clock()", e)
        print(f"[ClockError] Could not get market status: {e}")
        return False

def safe_get_bars(symbol: str, timeframe: TimeFrame, limit: int = 5):
    if not is_market_open():
        print(f"[MarketClosed] Skipping get_bars for {symbol}")
        return None
    try:
        bars = safe_api_call(api.get_bars, symbol, timeframe, limit=limit).df
        return bars
    except Exception as e:
        print(f"[safe_get_bars] Failed for {symbol}: {e}")
        return None

def safe_get_option_contracts(symbol: str):
    if not is_market_open():
        print(f"[MarketClosed] Skipping get_options_contracts for {symbol}")
        return []
    try:
        return safe_api_call(api.get_options_contracts, symbol)
    except Exception as e:
        print(f"[safe_get_option_contracts] Failed for {symbol}: {e}")
        return []

# -------------------------
# UNIVERSE SELECTION
# -------------------------
def get_universe() -> List[str]:
    if not is_market_open():
        print("[MarketClosed] Skipping universe selection")
        return []
    selected = []
    try:
        assets = safe_api_call(api.list_assets, status="active")
    except Exception as e:
        print(f"[Error] Could not list assets: {e}")
        send_critical_alert("Failed to list assets", e)
        return []

    tradable = [a for a in assets if getattr(a, "tradable", False) and getattr(a, "exchange", "") in ("NASDAQ", "NYSE")]

    for a in tradable:
        sym = a.symbol
        try:
            bars = safe_get_bars(sym, TimeFrame.Day, limit=5)
            if bars is None or bars.empty:
                continue
            last_bar_time = bars.index[-1].to_pydatetime()
            if (datetime.now() - last_bar_time).days > 3:
                continue
            close = float(bars["close"].iloc[-1])
            vol = int(bars["volume"].iloc[-1])
            if MIN_PRICE <= close <= MAX_PRICE and vol >= MIN_VOLUME:
                selected.append((sym, vol))
        except Exception as e:
            # continue on per-symbol errors
            print(f"[UniverseError] {sym}: {e}")
            continue

    top_symbols = sorted(selected, key=lambda x: x[1], reverse=True)[:TOP_N_STOCKS]
    chosen = [s[0] for s in top_symbols]
    print(f"[Universe] Selected {len(chosen)} symbols: {chosen}")
    try:
        send_discord_message(f"📈 Universe updated: {chosen}")
    except Exception:
        pass
    return chosen

# -------------------------
# OPTION SELECTION
# -------------------------
def choose_atm_call_put(symbol: str):
    if not is_market_open():
        print(f"[MarketClosed] Skipping option selection for {symbol}")
        return None, None, None
    try:
        contracts = safe_get_option_contracts(symbol)
        if not contracts:
            return None, None, None

        bars = safe_get_bars(symbol, TimeFrame.Day, limit=1)
        if bars is None or bars.empty:
            return None, None, None
        underlying_price = float(bars["close"].iloc[-1])

        today = datetime.now().date()
        valid_contracts = []
        for c in contracts:
            try:
                exp_str = getattr(c, "expiration_date", None) or getattr(c, "expiration", None)
                if not exp_str:
                    continue
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                if (exp_date - today).days < MIN_DAYS_TO_EXPIRY:
                    continue
                vol = int(getattr(c, "volume", 0) or 0)
                last_price = getattr(c, "last_trade_price", None) or getattr(c, "ask_price", None)
                if not last_price:
                    continue
                last_price = float(last_price)
                if last_price < MIN_OPTION_PRICE or vol < MIN_OPTION_VOLUME:
                    continue
                valid_contracts.append(c)
            except Exception:
                continue

        if not valid_contracts:
            return None, None, underlying_price

        calls = [c for c in valid_contracts if getattr(c, "option_type", "").lower() == "call"]
        puts = [c for c in valid_contracts if getattr(c, "option_type", "").lower() == "put"]

        if not calls or not puts:
            return None, None, underlying_price

        atm_call = min(calls, key=lambda x: abs(float(getattr(x, "strike_price", getattr(x, "strike", 0))) - underlying_price))
        atm_put = min(puts, key=lambda x: abs(float(getattr(x, "strike_price", getattr(x, "strike", 0))) - underlying_price))

        return atm_call, atm_put, underlying_price
    except Exception as e:
        print(f"[OptionSelectError] {symbol}: {e}")
        return None, None, None

# -------------------------
# TRADING LOGIC
# -------------------------
def trade_logic():
    if not is_market_open():
        print("[MarketClosed] Skipping trade logic")
        return
    try:
        account = safe_api_call(api.get_account)
        cash = float(account.cash)
        if cash <= 0:
            print("[TradeLogic] No cash available.")
            return
        max_invest = cash * RISK_PER_TRADE

        global symbols
        symbols = get_universe()
        if not symbols:
            print("[TradeLogic] No symbols selected.")
            return

        for sym in symbols:
            call, put, underlying_price = choose_atm_call_put(sym)
            if not call or not put:
                print(f"[{sym}] No valid ATM options.")
                continue

            call_price = float(getattr(call, "ask_price", getattr(call, "last_trade_price", 0)))
            put_price = float(getattr(put, "ask_price", getattr(put, "last_trade_price", 0)))

            # Prevent division by zero or unreasonable qty
            if call_price <= 0 or put_price <= 0:
                print(f"[{sym}] Invalid option price. call_price={call_price}, put_price={put_price}")
                continue

            call_qty = int(max(0, int(max_invest / (call_price * 100))))
            put_qty = int(max(0, int(max_invest / (put_price * 100))))

            if call_qty >= 1 and getattr(call, "symbol", None) and call.symbol not in purchased_options:
                try:
                    safe_api_call(api.submit_order, symbol=call.symbol, qty=call_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(call.symbol)
                    msg = f"🟢 Bought CALL {call.symbol} x{call_qty} at ~${call_price:.2f}"
                    print(f"[TRADE] {msg}")
                    send_discord_message(msg)
                except Exception as e:
                    print(f"[OrderError] CALL {getattr(call, 'symbol', 'unknown')}: {e}")
                    send_discord_message(f"❌ OrderError CALL {getattr(call, 'symbol', 'unknown')}: {e}")

            if put_qty >= 1 and getattr(put, "symbol", None) and put.symbol not in purchased_options:
                try:
                    safe_api_call(api.submit_order, symbol=put.symbol, qty=put_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(put.symbol)
                    msg = f"🔴 Bought PUT {put.symbol} x{put_qty} at ~${put_price:.2f}"
                    print(f"[TRADE] {msg}")
                    send_discord_message(msg)
                except Exception as e:
                    print(f"[OrderError] PUT {getattr(put, 'symbol', 'unknown')}: {e}")
                    send_discord_message(f"❌ OrderError PUT {getattr(put, 'symbol', 'unknown')}: {e}")

    except Exception as e:
        print(f"[TradeLogicError] {e}")
        send_critical_alert("Trade logic failure", e)

# -------------------------
# RISK MANAGEMENT
# -------------------------
def manage_risk():
    if not is_market_open():
        print("[MarketClosed] Skipping risk management")
        return
    try:
        positions = safe_api_call(api.list_positions)
    except Exception as e:
        print(f"[ManageRisk] Could not fetch positions: {e}")
        send_critical_alert("Failed to list positions", e)
        return

    for pos in positions:
        try:
            if getattr(pos, "asset_class", "") != "option":
                continue
            symbol = pos.symbol
            qty = int(abs(float(pos.qty)))
            entry_price = float(pos.avg_entry_price)
            current_price = float(pos.current_price)
            if current_price < entry_price * (1 - STOP_LOSS_PCT):
                safe_api_call(api.submit_order, symbol=symbol, qty=qty, side="sell", type="market", time_in_force="day")
                print(f"[STOP-LOSS] Sold {symbol} at {current_price:.2f}")
                send_discord_message(f"⚠️ STOP-LOSS triggered: Sold {symbol} at ${current_price:.2f}")
        except Exception as e:
            print(f"[RiskError] {e}")
            # continue checking other positions

# -------------------------
# DAILY RESET
# -------------------------
def reset_daily_state():
    global purchased_options
    try:
        account = safe_api_call(api.get_account)
        positions = safe_api_call(api.list_positions)
        send_discord_message(
            f"📅 Daily Summary: Equity ${float(account.equity):,.2f} | "
            f"Cash ${float(account.cash):,.2f} | Positions {len(positions)}"
        )
    except Exception as e:
        print(f"[DailyResetError] {e}")
        send_discord_message(f"⚠️ Could not fetch daily summary: {e}")
    purchased_options = set()
    print("[DailyReset] Cleared purchased options.")

# -------------------------
# HEARTBEAT
# -------------------------
def send_heartbeat():
    try:
        account = safe_api_call(api.get_account)
        equity = float(account.equity)
        cash = float(account.cash)
        send_discord_message(f"💓 Bot alive at {datetime.now():%H:%M}. Equity: ${equity:,.2f} | Cash: ${cash:,.2f}")
    except Exception as e:
        print(f"[HeartbeatError] {e}")
        send_discord_message(f"⚠️ Heartbeat failed: {e}")

# -------------------------
# SCHEDULER
# -------------------------
def daily_routine():
    print(f"[DailyRoutine] Running at {datetime.now()}")
    trade_logic()
    # schedule risk management to run every TRADE_INTERVAL_MINUTES from now
    schedule.every(TRADE_INTERVAL_MINUTES).minutes.do(manage_risk)

def schedule_jobs():
    schedule.every().day.at(DAILY_REFRESH_TIME).do(lambda: (print("[Schedule] Refreshing universe..."), get_universe()))
    schedule.every().day.at(DAILY_TRADE_TIME).do(daily_routine)
    schedule.every().day.at("20:00").do(reset_daily_state)
    schedule.every(3).hours.do(send_heartbeat)

# -------------------------
# SIGNALS & EXCEPTIONS (monitoring)
# -------------------------
def handle_termination(signum, frame):
    send_critical_alert(f"Process received termination signal ({signum}). Shutting down gracefully.")
    print(f"[Signal] Received signal {signum}. Exiting.")
    sys.exit(0)

def excepthook(type_, value, tb):
    # Unhandled exception handler: send to Discord and then print to console
    trace = "".join(traceback.format_exception(type_, value, tb))
    print(f"[UncaughtException] {trace}")
    try:
        send_discord_message(f"🔥 Uncaught exception: {value}\n```{trace[-1900:]}```", critical=True)
    except Exception:
        pass
    # Optionally exit so Render can restart the service (recommended)
    sys.exit(1)

# register handlers
signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_termination)
sys.excepthook = excepthook

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print("🚀 Dynamic Options Swing (Alpaca) started", datetime.now())
    try:
        send_discord_message(f"🚀 Bot started at {datetime.now():%Y-%m-%d %H:%M}")
    except Exception:
        print("[Startup] Discord start message failed or webhook missing.")

    schedule_jobs()

    # Run one immediate pass, then enter main scheduling loop
    try:
        trade_logic()
        manage_risk()
    except Exception as e:
        print(f"[StartupError] {e}")
        send_critical_alert("StartupError", e)

    # Main loop with exception capture so we can alert on unexpected crashes
    try:
        while True:
            try:
                schedule.run_pending()
            except Exception as loop_exc:
                # Any exception in scheduled jobs should be alerted
                print(f"[SchedulerError] {loop_exc}")
                send_critical_alert("Scheduler job raised exception", loop_exc)
                # after alert, continue loop to attempt recovery
            time.sleep(1)
    except Exception as final_exc:
        # Fallback final alert
        send_critical_alert("Fatal error in main loop", final_exc)
        raise
