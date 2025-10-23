import os
import time
import schedule
from datetime import datetime
from typing import List, Tuple, Optional
from alpaca_trade_api import REST, TimeFrame
from alpaca_trade_api.rest import APIError

# ======================
# CONFIGURATION
# ======================
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

if not all([API_KEY, API_SECRET, BASE_URL]):
    raise SystemExit("❌ Missing Alpaca API credentials. Check Render environment variables.")

api = REST(API_KEY, API_SECRET, BASE_URL)

RISK_PER_TRADE = 0.02
STOP_LOSS_PCT = 0.35
TOP_N_STOCKS = 10
MIN_PRICE = 3.0
MAX_PRICE = 50.0
MIN_VOLUME = 1_000_000
MIN_OPTION_VOLUME = 50
MIN_OPTION_PRICE = 0.5
MIN_DAYS_TO_EXPIRY = 3

MAX_RETRIES = 2
RETRY_SLEEP = 2
MAX_ASSETS_TO_CHECK = 500  # avoid looping through thousands of tickers

DAILY_REFRESH_TIME = "09:45"
DAILY_TRADE_TIME = "09:50"
MANAGE_RISK_INTERVAL_MINUTES = 10

symbols: List[str] = []
purchased_options = set()

# ======================
# SAFE API HELPERS
# ======================
def safe_get_bars(symbol: str, timeframe: TimeFrame, limit: int = 5):
    """Fetch bars with limited retries and skip permanently if not found."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            bars = api.get_bars(symbol, timeframe, limit=limit).df
            return bars
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                print(f"[Skip] {symbol} has no bar data.")
                return None
            print(f"[Retry {attempt}/{MAX_RETRIES}] Bars failed for {symbol}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
    return None

def safe_get_option_contracts(symbol: str):
    try:
        return api.get_options_contracts(symbol)
    except Exception as e:
        print(f"[Warn] No options data for {symbol}: {e}")
        return []

# ======================
# UNIVERSE SELECTION
# ======================
def get_universe() -> List[str]:
    """Select liquid tickers with proper price and volume."""
    print("[Universe] Refreshing symbol list...")
    try:
        all_assets = api.list_assets(status="active")
    except Exception as e:
        print(f"[Error] Could not list assets: {e}")
        return []

    tradable = [
        a for a in all_assets
        if getattr(a, "tradable", False)
        and getattr(a, "exchange", "") in ("NASDAQ", "NYSE")
        and not getattr(a, "symbol", "").endswith(("W", "U", "R", "."))
        and "ETF" not in getattr(a, "name", "").upper()
    ]

    selected = []
    for a in tradable[:MAX_ASSETS_TO_CHECK]:
        sym = a.symbol
        bars = safe_get_bars(sym, TimeFrame.Day, limit=5)
        if bars is None or bars.empty:
            continue
        close = float(bars["close"].iloc[-1])
        vol = int(bars["volume"].iloc[-1])
        if MIN_PRICE <= close <= MAX_PRICE and vol >= MIN_VOLUME:
            selected.append((sym, vol))

    top = sorted(selected, key=lambda x: x[1], reverse=True)[:TOP_N_STOCKS]
    chosen = [s[0] for s in top]
    print(f"[Universe] Selected {len(chosen)} symbols: {chosen}")
    return chosen

# ======================
# OPTION SELECTION
# ======================
def choose_atm_call_put(symbol: str) -> Tuple[Optional[object], Optional[object], Optional[float]]:
    try:
        contracts = safe_get_option_contracts(symbol)
        if not contracts:
            return None, None, None

        bars = safe_get_bars(symbol, TimeFrame.Day, limit=1)
        if bars is None or bars.empty:
            return None, None, None
        underlying_price = float(bars["close"].iloc[-1])

        valid = []
        today = datetime.now().date()
        for c in contracts:
            exp = getattr(c, "expiration_date", None) or getattr(c, "expiration", None)
            if not exp:
                continue
            try:
                exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            except Exception:
                continue
            if (exp_date - today).days < MIN_DAYS_TO_EXPIRY:
                continue
            price = getattr(c, "last_trade_price", None) or getattr(c, "ask_price", None)
            vol = getattr(c, "volume", 0)
            if price and float(price) >= MIN_OPTION_PRICE and int(vol) >= MIN_OPTION_VOLUME:
                valid.append(c)

        if not valid:
            return None, None, underlying_price

        calls = [c for c in valid if getattr(c, "option_type", "").lower() == "call"]
        puts = [c for c in valid if getattr(c, "option_type", "").lower() == "put"]
        if not calls or not puts:
            return None, None, underlying_price

        atm_call = min(calls, key=lambda x: abs(float(getattr(x, "strike_price", 0)) - underlying_price))
        atm_put = min(puts, key=lambda x: abs(float(getattr(x, "strike_price", 0)) - underlying_price))
        return atm_call, atm_put, underlying_price
    except Exception as e:
        print(f"[OptionError] {symbol}: {e}")
        return None, None, None

# ======================
# TRADING LOGIC
# ======================
def trade_logic():
    try:
        account = api.get_account()
        cash = float(account.cash)
        if cash <= 0:
            print("[TradeLogic] No cash.")
            return
        max_invest = cash * RISK_PER_TRADE

        global symbols
        symbols = get_universe()
        if not symbols:
            print("[TradeLogic] No symbols.")
            return

        for sym in symbols:
            call, put, price = choose_atm_call_put(sym)
            if not call or not put or not price:
                continue

            def ask(c):
                return float(getattr(c, "ask_price", 0) or getattr(c, "last_trade_price", 0) or 0)

            call_price, put_price = ask(call), ask(put)
            if call_price <= 0 or put_price <= 0:
                continue

            call_qty = int(max_invest / (call_price * 100))
            put_qty = int(max_invest / (put_price * 100))

            for opt, qty, side in [(call, call_qty, "CALL"), (put, put_qty, "PUT")]:
                if qty < 1 or opt.symbol in purchased_options:
                    continue
                try:
                    api.submit_order(symbol=opt.symbol, qty=qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(opt.symbol)
                    print(f"[TRADE] Bought {side} {opt.symbol} x{qty}")
                except APIError as e:
                    print(f"[OrderError] {opt.symbol}: {e}")
    except Exception as e:
        print(f"[TradeLogicError] {e}")

# ======================
# RISK MANAGEMENT
# ======================
def manage_risk():
    try:
        for pos in api.list_positions():
            if getattr(pos, "asset_class", "") != "option":
                continue
            qty = int(abs(float(pos.qty)))
            entry, current = float(pos.avg_entry_price), float(pos.current_price)
            if current < entry * (1 - STOP_LOSS_PCT):
                api.submit_order(symbol=pos.symbol, qty=qty, side="sell", type="market", time_in_force="day")
                print(f"[STOP-LOSS] Sold {pos.symbol}")
    except Exception as e:
        print(f"[ManageRiskError] {e}")

# ======================
# SCHEDULER
# ======================
def reset_daily_state():
    global purchased_options
    purchased_options = set()
    print("[DailyReset] Cleared purchased options.")

def daily_routine():
    print(f"[DailyRoutine] Running at {datetime.now()}")
    trade_logic()
    schedule.every(MANAGE_RISK_INTERVAL_MINUTES).minutes.do(manage_risk)

def schedule_jobs():
    schedule.every().day.at(DAILY_REFRESH_TIME).do(lambda: (print("[Schedule] Refreshing..."), get_universe()))
    schedule.every().day.at(DAILY_TRADE_TIME).do(daily_routine)
    schedule.every().day.at("20:00").do(reset_daily_state)

# ======================
# MAIN LOOP
# ======================
if __name__ == "__main__":
    print("🚀 Dynamic Options Swing started.", datetime.now())
    schedule_jobs()
    try:
        trade_logic()
        manage_risk()
    except Exception as e:
        print(f"[StartupError] {e}")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print(f"[SchedulerError] {e}")
            time.sleep(5)

