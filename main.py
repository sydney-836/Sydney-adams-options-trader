#!/usr/bin/env python3
"""
Dynamic Options Swing (Alpaca) with Discord notifications
 - Dynamic universe selection (price / volume filters)
 - ATM call/put selection with option-volume & price filters
 - Exponential backoff for data calls, skip symbol after retries (no Discord alerts for data failures)
 - Discord: heartbeats, trade updates, stop-loss, daily summary, critical alerts on crashes
 - Safe retry wrapper for Alpaca calls
"""
import os
import sys
import time
import traceback
import signal
import schedule
import requests
from datetime import datetime, timedelta
from typing import List, Optional
from alpaca_trade_api import REST, TimeFrame
from alpaca_trade_api.rest import APIError

# -------------------------
# CONFIGURATION (tweakable)
# -------------------------
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Universe & option filters
RISK_PER_TRADE = 0.02
STOP_LOSS_PCT = 0.35
TOP_N_STOCKS = 10            # choose top N by volume from the filtered universe
MIN_PRICE = 3.0
MAX_PRICE = 50.0
MIN_VOLUME = 1_000_000       # min daily volume for the stock
MIN_OPTION_VOLUME = 50
MIN_OPTION_PRICE = 0.50
MIN_DAYS_TO_EXPIRY = 3

# Retry / timing
MAX_RETRIES = 3               # attempts for each data call
INITIAL_RETRY_SLEEP = 3      # seconds (will double each retry)
TRADE_INTERVAL_MINUTES = 30

# Scheduler times (market timezone assumed)
DAILY_REFRESH_TIME = "09:45"
DAILY_TRADE_TIME = "09:50"
DAILY_SUMMARY_TIME = "20:00"

# -------------------------
# ALPACA CLIENT
# -------------------------
api = REST(API_KEY, API_SECRET, BASE_URL)

# -------------------------
# STATE
# -------------------------
symbols: List[str] = []
purchased_options = set()     # track option symbols we've bought today to avoid doubles

# -------------------------
# DISCORD HELPERS
# -------------------------
def send_discord_message(message: str, critical: bool = False) -> None:
    """Send message to Discord webhook (if set). Small timeout so it doesn't block."""
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not set; skipping message.")
        return
    payload = {"content": ("@here\n" if critical else "") + message}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=6)
        if resp.status_code not in (200, 204):
            print(f"[Discord] non-2xx response: {resp.status_code}: {resp.text}")
    except Exception as e:
        # intentionally do not escalate network errors from Discord helper
        print(f"[Discord] send error: {e}")

def send_critical_alert(title: str, exc: Optional[BaseException] = None) -> None:
    """Send a critical alert (with truncated stack trace) to Discord."""
    try:
        trace = ""
        if exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            trace = f"\n```{tb[-1800:]}```"
        else:
            tb = "".join(traceback.format_stack())
            trace = f"\n```{tb[-1800:]}```"
        send_discord_message(f"🔥 CRITICAL: {title} at {datetime.now():%Y-%m-%d %H:%M:%S}{trace}", critical=True)
    except Exception as e:
        print(f"[CriticalAlertError] {e}")

# -------------------------
# SAFE API CALLS / BACKOFF
# -------------------------
def safe_api_call(fn, *args, max_retries=MAX_RETRIES, initial_sleep=INITIAL_RETRY_SLEEP, **kwargs):
    """
    Generic safe wrapper with exponential backoff.
    Raises the last exception if all retries fail.
    """
    sleep = initial_sleep
    for attempt in range(1, max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # Don't spam Discord for transient data fetch failures;
            # only print here. Critical alerts managed by callers when appropriate.
            print(f"[Retry {attempt}/{max_retries}] {fn.__name__} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(sleep)
            print(f"[Retry] sleeping {sleep}s before next attempt for {fn.__name__}...")
            sleep *= 2

# -------------------------
# MARKET STATUS / DATA HELPERS
# -------------------------
def is_market_open() -> bool:
    """Return True if market is open (uses safe_api_call)."""
    try:
        clock = safe_api_call(api.get_clock)
        return getattr(clock, "is_open", False)
    except Exception as e:
        # alert only on persistent auth issues
        if "401" in str(e):
            send_critical_alert("401 Unauthorized from Alpaca during get_clock()", e)
        print(f"[ClockError] {e}")
        return False

def fetch_bars_with_backoff(symbol: str, timeframe: TimeFrame, limit: int = 5) -> Optional[object]:
    """
    Fetch bars (returns dataframe-like object when available).
    If fails after retries, returns None (no Discord alert here per user request).
    """
    try:
        # safe_api_call will raise if all retries fail
        bars = safe_api_call(api.get_bars, symbol, timeframe, limit=limit)
        # .df may exist depending on the alpaca client version
        if hasattr(bars, "df"):
            return bars.df
        return bars
    except Exception as e:
        print(f"[DataFetch] Max retries exceeded for {symbol} bars: {e}")
        return None

def fetch_option_contracts_with_backoff(symbol: str):
    """Return option contracts list or None on persistent failure (no discord alert)."""
    try:
        contracts = safe_api_call(api.get_options_contracts, symbol)
        return contracts
    except Exception as e:
        print(f"[DataFetch] Max retries exceeded for {symbol} option contracts: {e}")
        return None

# -------------------------
# UNIVERSE SELECTION (dynamic)
# -------------------------
def get_universe() -> List[str]:
    """
    Build dynamic universe:
      - Active tradable assets on NASDAQ/NYSE
      - Price between MIN_PRICE and MAX_PRICE
      - Last daily volume >= MIN_VOLUME
      - Keep top TOP_N_STOCKS by volume
    """
    chosen: List[str] = []
    try:
        assets = safe_api_call(api.list_assets, status="active")
    except Exception as e:
        print(f"[Universe] list_assets failed: {e}")
        send_critical_alert("Failed to list assets", e)
        return []

    tradable = [a for a in assets if getattr(a, "tradable", False) and getattr(a, "exchange", "") in ("NASDAQ", "NYSE")]
    candidates = []
    for a in tradable:
        sym = a.symbol
        try:
            bars = fetch_bars_with_backoff(sym, TimeFrame.Day, limit=2)
            if bars is None or bars.empty:
                continue
            # Use last close & last volume
            last_close = float(bars["close"].iloc[-1])
            last_vol = int(bars["volume"].iloc[-1])
            # exclude stale data
            last_bar_time = bars.index[-1].to_pydatetime()
            if (datetime.now() - last_bar_time).days > 3:
                continue
            if MIN_PRICE <= last_close <= MAX_PRICE and last_vol >= MIN_VOLUME:
                candidates.append((sym, last_vol))
        except Exception as e:
            # per-symbol issues: skip quietly (no Discord alert)
            print(f"[Universe] skipping {sym} due to error: {e}")
            continue

    # pick top by volume
    top = sorted(candidates, key=lambda x: x[1], reverse=True)[:TOP_N_STOCKS]
    chosen = [s for s, _ in top]
    print(f"[Universe] Selected {len(chosen)} symbols: {chosen}")
    # Inform Discord of updated universe (not for data failures)
    if chosen:
        send_discord_message(f"📈 Universe updated: {chosen}")
    return chosen

# -------------------------
# OPTION SELECTION (ATM)
# -------------------------
def choose_atm_call_put(symbol: str):
    """
    Return (call_contract, put_contract, underlying_price) or (None, None, price) when not found.
    Contracts must meet MIN_OPTION_VOLUME and MIN_OPTION_PRICE and expiry >= MIN_DAYS_TO_EXPIRY.
    """
    try:
        contracts = fetch_option_contracts_with_backoff(symbol)
        if not contracts:
            return None, None, None

        bars = fetch_bars_with_backoff(symbol, TimeFrame.Day, limit=1)
        if bars is None or bars.empty:
            return None, None, None
        underlying_price = float(bars["close"].iloc[-1])

        today = datetime.now().date()
        valid_contracts = []
        for c in contracts:
            try:
                # many Alpaca option objects use attributes like expiration_date, strike_price, option_type, volume, last_trade_price
                exp_str = getattr(c, "expiration_date", None) or getattr(c, "expiration", None)
                if not exp_str:
                    continue
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                if (exp_date - today).days < MIN_DAYS_TO_EXPIRY:
                    continue
                vol = int(getattr(c, "volume", 0) or 0)
                last_price = getattr(c, "last_trade_price", None) or getattr(c, "ask_price", None) or getattr(c, "last_price", None)
                if last_price is None:
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
        atm_put = min(puts,  key=lambda x: abs(float(getattr(x, "strike_price", getattr(x, "strike", 0))) - underlying_price))

        return atm_call, atm_put, underlying_price
    except Exception as e:
        print(f"[OptionSelectError] {symbol}: {e}")
        return None, None, None

# -------------------------
# TRADING LOGIC
# -------------------------
def trade_logic():
    """Main trade logic: build universe, select ATM options, and submit orders for calls & puts."""
    if not is_market_open():
        print("[MarketClosed] Skipping trade logic")
        return

    try:
        account = safe_api_call(api.get_account)
        cash = float(account.cash)
        if cash <= 0:
            print("[TradeLogic] No cash.")
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
                print(f"[{sym}] No valid ATM options meeting filters.")
                continue

            # fetch ask or last price
            call_price = float(getattr(call, "ask_price", getattr(call, "last_trade_price", getattr(call, "last_price", 0))))
            put_price  = float(getattr(put,  "ask_price", getattr(put,  "last_trade_price", getattr(put,  "last_price", 0))))

            # sanity checks
            if call_price <= 0 or put_price <= 0:
                print(f"[{sym}] invalid option price (call={call_price}, put={put_price}), skipping")
                continue

            call_qty = int(max_invest / (call_price * 100)) if call_price > 0 else 0
            put_qty  = int(max_invest / (put_price * 100)) if put_price > 0 else 0

            # BUY CALL
            call_symbol = getattr(call, "symbol", None) or getattr(call, "contract", None)
            if call_qty >= 1 and call_symbol and call_symbol not in purchased_options:
                try:
                    safe_api_call(api.submit_order, symbol=call_symbol, qty=call_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(call_symbol)
                    msg = f"🟢 Bought CALL {call_symbol} x{call_qty} at ~${call_price:.2f} (underlying {sym} @ ${underlying_price:.2f})"
                    print(f"[TRADE] {msg}")
                    send_discord_message(msg)
                except Exception as e:
                    print(f"[OrderError] CALL {call_symbol}: {e}")
                    send_discord_message(f"❌ OrderError CALL {call_symbol}: {e}")

            # BUY PUT
            put_symbol = getattr(put, "symbol", None) or getattr(put, "contract", None)
            if put_qty >= 1 and put_symbol and put_symbol not in purchased_options:
                try:
                    safe_api_call(api.submit_order, symbol=put_symbol, qty=put_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(put_symbol)
                    msg = f"🔴 Bought PUT {put_symbol} x{put_qty} at ~${put_price:.2f} (underlying {sym} @ ${underlying_price:.2f})"
                    print(f"[TRADE] {msg}")
                    send_discord_message(msg)
                except Exception as e:
                    print(f"[OrderError] PUT {put_symbol}: {e}")
                    send_discord_message(f"❌ OrderError PUT {put_symbol}: {e}")

    except Exception as e:
        print(f"[TradeLogicError] {e}")
        send_critical_alert("Trade logic failure", e)

# -------------------------
# RISK MANAGEMENT
# -------------------------
def manage_risk():
    """Check option positions and close if stop-loss condition met."""
    if not is_market_open():
        print("[MarketClosed] Skipping risk management")
        return
    try:
        positions = safe_api_call(api.list_positions)
    except Exception as e:
        print(f"[ManageRisk] Failed to list positions: {e}")
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
            continue

# -------------------------
# DAILY RESET & SUMMARY
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
        # user asked: no discord alerts for data fetch fails — but daily summary is allowed.
        print(f"[DailyResetError] {e}")
        send_discord_message(f"⚠️ Could not fetch daily summary: {e}")
    purchased_options = set()
    print("[DailyReset] Cleared purchased options for the day.")

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
        # don't spam discord if account fetch fails repeatedly

# -------------------------
# SCHEDULER
# -------------------------
def daily_routine():
    print(f"[DailyRoutine] Running at {datetime.now()}")
    trade_logic()
    # schedule risk management every TRADE_INTERVAL_MINUTES from now
    schedule.every(TRADE_INTERVAL_MINUTES).minutes.do(manage_risk)

def schedule_jobs():
    schedule.every().day.at(DAILY_REFRESH_TIME).do(lambda: (print("[Schedule] Refreshing universe..."), get_universe()))
    schedule.every().day.at(DAILY_TRADE_TIME).do(daily_routine)
    schedule.every().day.at(DAILY_SUMMARY_TIME).do(reset_daily_state)
    schedule.every(3).hours.do(send_heartbeat)

# -------------------------
# SIGNALS & EXCEPTIONS (monitoring)
# -------------------------
def handle_termination(signum, frame):
    try:
        send_critical_alert(f"Process received termination signal ({signum}). Shutting down gracefully.")
    except Exception:
        pass
    print(f"[Signal] Received signal {signum}. Exiting.")
    sys.exit(0)

def excepthook(type_, value, tb):
    trace = "".join(traceback.format_exception(type_, value, tb))
    print(f"[UncaughtException] {trace}")
    try:
        send_discord_message(f"🔥 Uncaught exception: {value}\n```{trace[-1800:]}```", critical=True)
    except Exception:
        pass
    sys.exit(1)

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
        print("[Startup] Discord start message failed (webhook missing or network)")

    schedule_jobs()

    try:
        # initial run
        trade_logic()
        manage_risk()
    except Exception as e:
        print(f"[StartupError] {e}")
        send_critical_alert("StartupError", e)

    # main loop
    try:
        while True:
            try:
                schedule.run_pending()
            except Exception as loop_exc:
                print(f"[SchedulerError] {loop_exc}")
                send_critical_alert("Scheduler job raised exception", loop_exc)
            time.sleep(1)
    except Exception as final_exc:
        send_critical_alert("Fatal error in main loop", final_exc)
        raise
