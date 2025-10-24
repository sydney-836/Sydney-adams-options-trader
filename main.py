#!/usr/bin/env python3
"""
Dynamic Options Swing (Alpaca) - Paper Trading with Hardcoded Universe
Fully integrated version: ATM option selection, trading, risk management, Discord notifications
"""
import os
import sys
import time
import traceback
import schedule
import requests
from datetime import datetime, timezone
from typing import List, Optional
from alpaca_trade_api import REST, TimeFrame

# -------------------------
# CONFIGURATION
# -------------------------
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

RISK_PER_TRADE = 0.02
STOP_LOSS_PCT = 0.35
MIN_PRICE = 3.0
MAX_PRICE = 50.0
MIN_VOLUME = 1_000_000
MIN_OPTION_VOLUME = 50
MIN_OPTION_PRICE = 0.50
MIN_DAYS_TO_EXPIRY = 3

MAX_RETRIES = 3
INITIAL_RETRY_SLEEP = 3
TRADE_INTERVAL_MINUTES = 30

# Hardcoded top 30 tickers for testing
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
            time.sleep(sleep)
            sleep *= 2

# -------------------------
# MARKET STATUS / DATA HELPERS
# -------------------------
def is_market_open() -> bool:
    try:
        clock = safe_api_call(api.get_clock)
        return getattr(clock, "is_open", False)
    except Exception as e:
        print(f"[ClockError] {e}")
        return False

def fetch_bars_with_backoff(symbol: str, timeframe: TimeFrame, limit: int = 5):
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

# -------------------------
# OPTIONS API
# -------------------------
def fetch_option_contracts_with_backoff(symbol: str):
    BASE_URL_OPTS = "https://paper-api.alpaca.markets/v2/options/contracts"
    headers = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": API_SECRET}
    params = {"symbol": symbol, "limit": 50}

    sleep = INITIAL_RETRY_SLEEP
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL_OPTS, headers=headers, params=params, timeout=6)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            return data
        except Exception as e:
            print(f"[Retry {attempt}/{MAX_RETRIES}] Failed fetching {symbol} options: {e}")
            if attempt == MAX_RETRIES:
                return None
            time.sleep(sleep)
            sleep *= 2
    return None

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
                exp_date_str = c.get("expiration_date")
                if not exp_date_str:
                    continue
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                if (exp_date - today).days < MIN_DAYS_TO_EXPIRY:
                    continue

                vol = int(c.get("volume") or 0)
                last_price = float(c.get("last_trade_price") or c.get("ask_price") or 0)
                if last_price < MIN_OPTION_PRICE or vol < MIN_OPTION_VOLUME:
                    continue
                valid_contracts.append(c)
            except Exception:
                continue

        if not valid_contracts:
            return None, None, underlying_price

        calls = [c for c in valid_contracts if c.get("option_type", "").lower() == "call"]
        puts  = [c for c in valid_contracts if c.get("option_type", "").lower() == "put"]

        if not calls or not puts:
            return None, None, underlying_price

        atm_call = min(calls, key=lambda x: abs(float(x.get("strike_price", 0)) - underlying_price))
        atm_put  = min(puts,  key=lambda x: abs(float(x.get("strike_price", 0)) - underlying_price))

        return atm_call, atm_put, underlying_price
    except Exception as e:
        print(f"[OptionSelectError] {symbol}: {e}")
        return None, None, None

# -------------------------
# ORDER SUBMISSION
# -------------------------
def submit_option_order(contract: dict, max_invest: float, purchased_options: set, side: str):
    try:
        symbol = contract.get("symbol")
        last_price = float(contract.get("ask_price") or contract.get("last_trade_price") or 0)
        if not symbol or last_price <= 0:
            print(f"[OrderSkip] Invalid symbol or price: {symbol}, {last_price}")
            return

        qty = int(max_invest / (last_price * 100))
        if qty < 1 or symbol in purchased_options:
            return

        safe_api_call(
            api.submit_order,
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force="day"
        )
        purchased_options.add(symbol)
        action = "🟢 Bought" if side == "buy" else "🔴 Sold"
        msg = f"{action} {symbol} x{qty} @ ~${last_price:.2f}"
        print(f"[TRADE] {msg}")
        send_discord_message(msg)
    except Exception as e:
        print(f"[OrderError] {symbol}: {e}")
        send_discord_message(f"❌ OrderError {symbol}: {e}")

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

        for sym in HARDCODED_TICKERS:
            call, put, underlying_price = choose_atm_call_put(sym)
            if not call or not put:
                print(f"[{sym}] No valid ATM options meeting filters.")
                continue

            submit_option_order(call, max_invest, purchased_options, side="buy")
            submit_option_order(put,  max_invest, purchased_options, side="buy")

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
            loss_pct = (entry_price - current_price) / entry_price

            if loss_pct >= STOP_LOSS_PCT:
                safe_api_call(
                    api.submit_order,
                    symbol=symbol,
                    qty=qty,
                    side="sell",
                    type="market",
                    time_in_force="day"
                )
                print(f"[STOP-LOSS] Sold {symbol} at {current_price:.2f} (loss {loss_pct*100:.1f}%)")
                send_discord_message(
                    f"⚠️ STOP-LOSS triggered: Sold {symbol} x{qty} at ${current_price:.2f} "
                    f"(loss {loss_pct*100:.1f}%)"
                )
            else:
                print(f"[Risk] {symbol} is safe: current {current_price:.2f}, entry {entry_price:.2f}")

        except Exception as e:
            print(f"[RiskError] {pos.symbol}: {e}")

# -------------------------
# SCHEDULER
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
