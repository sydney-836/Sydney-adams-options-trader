print("API_KEY:", API_KEY, "API_SECRET:", API_SECRET, "BASE_URL:", BASE_URL)
import os
import time
import schedule
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
# HELPERS
# -------------------------
def is_market_open() -> bool:
    """Return True if market is currently open."""
    try:
        clock = api.get_clock()
        return clock.is_open
    except Exception as e:
        print(f"[ClockError] Could not get market status: {e}")
        return False

def safe_get_bars(symbol: str, timeframe: TimeFrame, limit: int = 5):
    if not is_market_open():
        print(f"[MarketClosed] Skipping get_bars for {symbol}")
        return None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            bars = api.get_bars(symbol, timeframe, limit=limit).df
            return bars
        except Exception as e:
            print(f"[Retry {attempt}/{MAX_RETRIES}] get_bars failed for {symbol}: {e}")
            time.sleep(RETRY_SLEEP)
    return None

def safe_get_option_contracts(symbol: str):
    if not is_market_open():
        print(f"[MarketClosed] Skipping get_options_contracts for {symbol}")
        return []
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return api.get_options_contracts(symbol)
        except Exception as e:
            print(f"[Retry {attempt}/{MAX_RETRIES}] get_options_contracts failed for {symbol}: {e}")
            time.sleep(RETRY_SLEEP)
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
        assets = api.list_assets(status="active")
    except Exception as e:
        print(f"[Error] Could not list assets: {e}")
        return []

    tradable = [a for a in assets if getattr(a,"tradable",False) and getattr(a,"exchange","") in ("NASDAQ","NYSE")]

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
        except Exception:
            continue

    top_symbols = sorted(selected, key=lambda x:x[1], reverse=True)[:TOP_N_STOCKS]
    chosen = [s[0] for s in top_symbols]
    print(f"[Universe] Selected {len(chosen)} symbols: {chosen}")
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
                exp_str = getattr(c,"expiration_date",None) or getattr(c,"expiration",None)
                if not exp_str:
                    continue
                exp_date = datetime.strptime(exp_str,"%Y-%m-%d").date()
                if (exp_date - today).days < MIN_DAYS_TO_EXPIRY:
                    continue
                vol = int(getattr(c,"volume",0) or 0)
                last_price = getattr(c,"last_trade_price",None) or getattr(c,"ask_price",None)
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

        calls = [c for c in valid_contracts if getattr(c,"option_type","").lower()=="call"]
        puts = [c for c in valid_contracts if getattr(c,"option_type","").lower()=="put"]

        if not calls or not puts:
            return None, None, underlying_price

        atm_call = min(calls, key=lambda x: abs(float(getattr(x,"strike_price",getattr(x,"strike",0)))-underlying_price))
        atm_put  = min(puts,  key=lambda x: abs(float(getattr(x,"strike_price",getattr(x,"strike",0)))-underlying_price))

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
        account = api.get_account()
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

            call_price = float(getattr(call,"ask_price",getattr(call,"last_trade_price",0)))
            put_price  = float(getattr(put,"ask_price",getattr(put,"last_trade_price",0)))

            call_qty = int(max_invest / (call_price*100))
            put_qty  = int(max_invest / (put_price*100))

            if call_qty >= 1 and call.symbol not in purchased_options:
                try:
                    api.submit_order(symbol=call.symbol, qty=call_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(call.symbol)
                    print(f"[TRADE] Bought CALL {call.symbol} x{call_qty} ~{call_price}")
                except APIError as e:
                    print(f"[OrderError] CALL {call.symbol}: {e}")

            if put_qty >= 1 and put.symbol not in purchased_options:
                try:
                    api.submit_order(symbol=put.symbol, qty=put_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(put.symbol)
                    print(f"[TRADE] Bought PUT {put.symbol} x{put_qty} ~{put_price}")
                except APIError as e:
                    print(f"[OrderError] PUT {put.symbol}: {e}")

    except Exception as e:
        print(f"[TradeLogicError] {e}")

# -------------------------
# RISK MANAGEMENT
# -------------------------
def manage_risk():
    if not is_market_open():
        print("[MarketClosed] Skipping risk management")
        return
    try:
        positions = api.list_positions()
    except Exception as e:
        print(f"[ManageRisk] Could not fetch positions: {e}")
        return

    for pos in positions:
        try:
            if getattr(pos,"asset_class","")!="option":
                continue
            symbol = pos.symbol
            qty = int(abs(float(pos.qty)))
            entry_price = float(pos.avg_entry_price)
            current_price = float(pos.current_price)
            if current_price < entry_price*(1-STOP_LOSS_PCT):
                api.submit_order(symbol=symbol, qty=qty, side="sell", type="market", time_in_force="day")
                print(f"[STOP-LOSS] Sold {symbol} at {current_price:.2f}")
        except Exception as e:
            print(f"[RiskError] {e}")

# -------------------------
# DAILY RESET
# -------------------------
def reset_daily_state():
    global purchased_options
    purchased_options = set()
    print("[DailyReset] Cleared purchased options.")

# -------------------------
# SCHEDULER
# -------------------------
def daily_routine():
    print(f"[DailyRoutine] Running at {datetime.now()}")
    trade_logic()
    schedule.every(TRADE_INTERVAL_MINUTES).minutes.do(manage_risk)

def schedule_jobs():
    schedule.every().day.at(DAILY_REFRESH_TIME).do(lambda: (print("[Schedule] Refreshing universe..."), get_universe()))
    schedule.every().day.at(DAILY_TRADE_TIME).do(daily_routine)
    schedule.every().day.at("20:00").do(reset_daily_state)

# -------------------------
# MAIN
# -------------------------
if __name__=="__main__":
    print("🚀 Dynamic Options Swing (Alpaca) started", datetime.now())
    schedule_jobs()

    try:
        trade_logic()
        manage_risk()
    except Exception as e:
        print(f"[StartupError] {e}")

