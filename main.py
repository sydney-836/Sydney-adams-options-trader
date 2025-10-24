#!/usr/bin/env python3
"""
Dynamic Options Swing (Alpaca) - Paper Trading with Hardcoded Universe
 - Hardcoded top 30 tickers for testing ($3-$50, high option activity)
 - ATM call/put selection with option-volume & price filters
 - Exponential backoff for data calls, skip symbol after retries
 - Discord: heartbeats, trade updates, stop-loss, daily summary, critical alerts
 - Timezone-safe: all bar timestamps are forced to UTC
"""
import os
import sys
import time
import traceback
import signal
import schedule
import requests
from datetime import datetime, timedelta, timezone
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

RISK_PER_TRADE = 0.02
STOP_LOSS_PCT = 0.35
TOP_N_STOCKS = 10  # not used in hardcoded mode
MIN_PRICE = 3.0
MAX_PRICE = 50.0
MIN_VOLUME = 1_000_000
MIN_OPTION_VOLUME = 50
MIN_OPTION_PRICE = 0.50
MIN_DAYS_TO_EXPIRY = 3

MAX_RETRIES = 3
INITIAL_RETRY_SLEEP = 3
TRADE_INTERVAL_MINUTES = 30

DAILY_REFRESH_TIME = "09:45"
DAILY_TRADE_TIME = "09:50"
DAILY_SUMMARY_TIME = "20:00"

# -------------------------
# HARDCODED UNIVERSE
# -------------------------
HARDCODED_TICKERS = [
    "AAPL", "MSFT", "AMD", "NVDA", "INTC", "TSLA", "COIN", "BA", "META", "NFLX",
    "BIDU", "F", "GM", "ZM", "PLTR", "UBER", "LYFT", "SQ", "PYPL", "PFE",
    "MRNA", "SPCE", "NOK", "T", "VZ", "GME", "AMC", "SNDL", "FUBO", "NKLA"
]

# -------------------------
# ALPACA CLIENT
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
    if not DISCORD_WEBHOOK_URL:
        print("[Discord] webhook not set; skipping message.")
        return
    payload = {"content": ("@here\n" if critical else "") + message}
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=6)
        if resp.status_code not in (200, 204):
            print(f"[Discord] non-2xx response: {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[Discord] send error: {e}")

def send_critical_alert(title: str, exc: Optional[BaseException] = None) -> None:
    try:
        trace = ""
        if exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            trace = f"\n```{tb[-1800:]}```"
        else:
            tb = "".join(traceback.format_stack())
            trace = f"\n```{tb[-1800:]}```"
        send_discord_message(f"🔥 CRITICAL: {title} at {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%SZ}{trace}", critical=True)
    except Exception as e:
        print(f"[CriticalAlertError] {e}")

# -------------------------
# SAFE API CALLS / BACKOFF
# -------------------------
def safe_api_call(fn, *args, max_retries=MAX_RETRIES, initial_sleep=INITIAL_RETRY_SLEEP, **kwargs):
    sleep = initial_sleep
    for attempt in range(1, max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            print(f"[Retry {attempt}/{max_retries}] {fn.__name__} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(sleep + attempt)
            sleep *= 2

# -------------------------
# MARKET STATUS / DATA HELPERS
# -------------------------
def is_market_open() -> bool:
    try:
        clock = safe_api_call(api.get_clock)
        return getattr(clock, "is_open", False)
    except Exception as e:
        if "401" in str(e):
            send_critical_alert("401 Unauthorized from Alpaca during get_clock()", e)
        print(f"[ClockError] {e}")
        return False

def fetch_bars_with_backoff(symbol: str, timeframe: TimeFrame, limit: int = 5) -> Optional[object]:
    try:
        bars = safe_api_call(api.get_bars, symbol, timeframe, limit=limit)
        df = getattr(bars, "df", None) or bars
        import pandas as pd
        if df is not None and hasattr(df, "index"):
            df.index = pd.to_datetime(df.index).tz_localize('UTC', ambiguous='NaT', nonexistent='NaT')
        return df
    except Exception as e:
        print(f"[DataFetch] Max retries exceeded for {symbol} bars: {e}")
        return None

def fetch_option_contracts_with_backoff(symbol: str):
    try:
        contracts = safe_api_call(api.get_options_contracts, symbol)
        return contracts
    except Exception as e:
        print(f"[DataFetch] Max retries exceeded for {symbol} option contracts: {e}")
        return None

# -------------------------
# UNIVERSE SELECTION
# -------------------------
def get_universe() -> List[str]:
    chosen = HARDCODED_TICKERS
    print(f"[Universe] Using hardcoded universe: {chosen}")
    send_discord_message(f"📈 Universe updated (hardcoded): {chosen}")
    return chosen

# -------------------------
# OPTION SELECTION (ATM)
# -------------------------
def choose_atm_call_put(symbol: str):
    try:
        contracts = fetch_option_contracts_with_backoff(symbol)
        if not contracts:
            return None, None, None
        bars = fetch_bars_with_backoff(symbol, TimeFrame.Day, limit=1)
        if bars is None or getattr(bars, "empty", False):
            return None, None, None
        underlying_price = float(bars["close"].iloc[-1])
        today = datetime.now(timezone.utc).date()
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
        atm_put  = min(puts,  key=lambda x: abs(float(getattr(x, "strike_price", getattr(x, "strike", 0))) - underlying_price))
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
            call_price = float(getattr(call, "ask_price", getattr(call, "last_trade_price", getattr(call, "last_price", 0))))
            put_price  = float(getattr(put,  "ask_price", getattr(put,  "last_trade_price", getattr(put,  "last_price", 0))))
            if call_price <= 0 or put_price <= 0:
                print(f"[{sym}] invalid option price, skipping")
                continue
            call_qty = int(max_invest / (call_price * 100)) if call_price > 0 else 0
            put_qty  = int(max_invest / (put_price * 100)) if put_price > 0 else 0
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
            print(f"[RiskError] {pos.symbol}: {e}")

# -------------------------
# SCHEDULE
# -------------------------
schedule.every(TRADE_INTERVAL_MINUTES).minutes.do(trade_logic)
schedule.every(TRADE_INTERVAL_MINUTES).minutes.do(manage_risk)

def run_scheduler():
    print(f"🚀 Dynamic Options Swing (Alpaca) started {datetime.now(timezone.utc)}")
    trade_logic()
    manage_risk()
    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except KeyboardInterrupt:
            print("[Shutdown] Exiting cleanly...")
            sys.exit(0)
        except Exception as e:
            print(f"[SchedulerError] {e}")
            send_critical_alert("Scheduler failure", e)

if __name__ == "__main__":
    run_scheduler()
