import os
import time
import schedule
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from alpaca_trade_api import REST, TimeFrame
from alpaca_trade_api.rest import APIError

# -------------------------
# Configuration / Settings
# -------------------------
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

# Risk / universe settings
RISK_PER_TRADE = 0.02        # fraction of cash to risk per trade
STOP_LOSS_PCT = 0.35         # stop-loss threshold (35%)
TOP_N_STOCKS = 10            # how many symbols to select daily
MIN_PRICE = 3.0
MAX_PRICE = 50.0
MIN_VOLUME = 1_000_000       # daily volume threshold
MIN_OPTION_VOLUME = 50       # min option volume to consider
MIN_OPTION_PRICE = 0.5       # min option last price to consider
MIN_DAYS_TO_EXPIRY = 3       # skip options that expire in fewer days
MAX_RETRIES = 3              # retries for transient API calls
RETRY_SLEEP = 3              # seconds between retries

# Scheduling (local server timezone)
# Run daily symbol refresh shortly after market open.
DAILY_REFRESH_TIME = "09:45"   # e.g., 09:45 local (adjust if Render uses UTC)
DAILY_TRADE_TIME   = "09:50"   # run trading logic once every market day after refresh
# also run manage_risk periodically while market open
MANAGE_RISK_INTERVAL_MINUTES = 10

# -------------------------
# Alpaca client
# -------------------------
api = REST(API_KEY, API_SECRET, BASE_URL)

# -------------------------
# State
# -------------------------
symbols: List[str] = []
purchased_options = set()   # track option contract symbols bought in the current day to avoid duplicates

# -------------------------
# Helpers: safe API wrappers
# -------------------------
def safe_get_bars(symbol: str, timeframe: TimeFrame, limit: int = 5):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            bars = api.get_bars(symbol, timeframe, limit=limit).df
            return bars
        except Exception as e:
            print(f"[Retry {attempt}/{MAX_RETRIES}] get_bars failed for {symbol}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
    return None

def safe_get_option_contracts(symbol: str):
    """Try to fetch option contracts for a symbol (with retries)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Alpaca python client naming may vary by version; this attempts the common endpoints.
            # If your client exposes a different function, replace this call accordingly.
            contracts = api.get_options_contracts(symbol)
            return contracts
        except Exception as e:
            print(f"[Retry {attempt}/{MAX_RETRIES}] get_options_contracts failed for {symbol}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
    return []

# -------------------------
# Universe selection
# -------------------------
def get_universe() -> List[str]:
    """
    Build a conservative, liquid universe:
    - Only tradable assets on NASDAQ/NYSE
    - Price between MIN_PRICE and MAX_PRICE
    - Volume > MIN_VOLUME
    - Skip symbols with no recent bars (older than 3 days)
    Returns top TOP_N_STOCKS by volume.
    """
    selected = []
    try:
        assets = api.list_assets(status="active")
    except Exception as e:
        print(f"[Error] Could not list assets: {e}")
        return []

    tradable = [a for a in assets if getattr(a, "tradable", False) and getattr(a, "exchange", "") in ("NASDAQ", "NYSE")]

    for a in tradable:
        sym = a.symbol
        try:
            bars = safe_get_bars(sym, TimeFrame.Day, limit=5)
            if bars is None or bars.empty:
                continue

            # skip if last bar older than 3 days
            last_bar_time = bars.index[-1].to_pydatetime()
            if (datetime.now() - last_bar_time).days > 3:
                # inactive / halted
                continue

            close = float(bars["close"].iloc[-1])
            vol = int(bars["volume"].iloc[-1])

            if MIN_PRICE <= close <= MAX_PRICE and vol >= MIN_VOLUME:
                selected.append((sym, vol))
        except Exception:
            # ignore per-symbol errors
            continue

    # sort by volume and return top N
    selected_sorted = sorted(selected, key=lambda x: x[1], reverse=True)[:TOP_N_STOCKS]
    chosen = [s[0] for s in selected_sorted]
    print(f"[Universe] Selected {len(chosen)} symbols: {chosen}")
    return chosen

# -------------------------
# Option selection
# -------------------------
def choose_atm_call_put(symbol: str) -> Tuple[Optional[object], Optional[object], Optional[float]]:
    """
    Use Alpaca option contracts to choose the nearest-ATM call and put for a near-term expiration.
    Returns (call_contract, put_contract, underlying_price) or (None,None,None).
    Contract object fields used below may depend on Alpaca client version:
      - contract.symbol  (option contract symbol)
      - contract.expiration_date (YYYY-MM-DD) or similar
      - contract.strike_price
      - contract.option_type or contract.side (call/put)
      - contract.last_trade_price or contract.last_quote or contract.ask_price
      - contract.volume
    Adjust attribute names if your Alpaca client returns differently.
    """
    try:
        # get the option contracts list for symbol
        contracts = safe_get_option_contracts(symbol)
        if not contracts:
            return None, None, None

        # get underlying latest price
        bars = safe_get_bars(symbol, TimeFrame.Day, limit=1)
        if bars is None or bars.empty:
            return None, None, None
        underlying_price = float(bars["close"].iloc[-1])

        # filter valid contracts (enough days to expiry)
        valid_contracts = []
        today = datetime.now().date()
        for c in contracts:
            try:
                # try multiple attribute names gracefully
                exp_date_str = getattr(c, "expiration_date", None) or getattr(c, "expiration", None)
                if not exp_date_str:
                    continue
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                days_to_expiry = (exp_date - today).days
                if days_to_expiry < MIN_DAYS_TO_EXPIRY:
                    continue

                # volume & price checks
                vol = int(getattr(c, "volume", 0) or 0)
                last_price = getattr(c, "last_trade_price", None) or getattr(c, "last_price", None) or getattr(c, "ask_price", None) or getattr(c, "midpoint", None)
                if last_price is None:
                    # skip if no price info
                    continue
                last_price = float(last_price)
                if last_price < MIN_OPTION_PRICE or vol < MIN_OPTION_VOLUME:
                    continue

                valid_contracts.append(c)
            except Exception:
                continue

        if not valid_contracts:
            return None, None, underlying_price

        # separate calls/puts (attribute names vary)
        calls = [c for c in valid_contracts if (getattr(c, "option_type", "").lower() == "call") or (getattr(c, "side", "").lower() == "call")]
        puts = [c for c in valid_contracts if (getattr(c, "option_type", "").lower() == "put") or (getattr(c, "side", "").lower() == "put")]

        if not calls or not puts:
            return None, None, underlying_price

        # choose ATM by strike proximity
        atm_call = min(calls, key=lambda x: abs(float(getattr(x, "strike_price", getattr(x, "strike", 0))) - underlying_price))
        atm_put  = min(puts,  key=lambda x: abs(float(getattr(x, "strike_price", getattr(x, "strike", 0))) - underlying_price))

        return atm_call, atm_put, underlying_price

    except Exception as e:
        print(f"[OptionSelectError] {symbol}: {e}")
        return None, None, None

# -------------------------
# Trading & risk management
# -------------------------
def trade_logic():
    """Main trading run: select universe, pick ATM options, place orders within risk limits."""
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
            if not call or not put or underlying_price is None:
                print(f"[{sym}] No valid ATM options.")
                continue

            # fetch ask prices (attribute names may vary)
            def get_ask(c):
                return float(getattr(c, "ask_price", None) or getattr(c, "ask", None) or getattr(c, "last_trade_price", None) or 0.0)

            call_ask = get_ask(call)
            put_ask = get_ask(put)

            if call_ask <= 0 or put_ask <= 0:
                continue

            # contracts trade in 100-shares per contract
            call_qty = int(max_invest / (call_ask * 100))
            put_qty  = int(max_invest / (put_ask * 100))

            # place call
            if call_qty >= 1 and call.symbol not in purchased_options:
                try:
                    api.submit_order(symbol=call.symbol, qty=call_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(call.symbol)
                    print(f"[TRADE] Bought CALL {call.symbol} x{call_qty} (~{call_ask:.2f}) underlying {sym}@{underlying_price:.2f}")
                except APIError as e:
                    print(f"[OrderError] Failed to place CALL order {call.symbol}: {e}")

            # place put
            if put_qty >= 1 and put.symbol not in purchased_options:
                try:
                    api.submit_order(symbol=put.symbol, qty=put_qty, side="buy", type="market", time_in_force="day")
                    purchased_options.add(put.symbol)
                    print(f"[TRADE] Bought PUT  {put.symbol} x{put_qty} (~{put_ask:.2f}) underlying {sym}@{underlying_price:.2f}")
                except APIError as e:
                    print(f"[OrderError] Failed to place PUT order {put.symbol}: {e}")

    except Exception as e:
        print(f"[TradeLogicError] {e}")

def manage_risk():
    """Check option positions and liquidate if price falls below stop-loss threshold."""
    try:
        positions = api.list_positions()
    except Exception as e:
        print(f"[ManageRisk] Could not fetch positions: {e}")
        return

    for pos in positions:
        try:
            if getattr(pos, "asset_class", "") != "option":
                continue

            symbol = pos.symbol
            qty = int(abs(float(pos.qty)))
            entry_price = float(pos.avg_entry_price)
            current_price = float(pos.current_price)

            if current_price <= 0:
                continue

            if current_price < entry_price * (1 - STOP_LOSS_PCT):
                try:
                    api.submit_order(symbol=symbol, qty=qty, side="sell", type="market", time_in_force="day")
                    print(f"[STOP-LOSS] Sold {symbol} qty={qty} current={current_price:.2f} entry={entry_price:.2f}")
                except APIError as e:
                    print(f"[StopLossOrderError] Failed to liquidate {symbol}: {e}")
        except Exception:
            continue

# -------------------------
# Daily reset: clear purchased set at midnight (or market close)
# -------------------------
def reset_daily_state():
    global purchased_options
    purchased_options = set()
    print("[DailyReset] Cleared purchased_options set.")

# -------------------------
# Scheduler wiring
# -------------------------
def daily_routine():
    """What to run once each market day: refresh universe, run one trade pass, then start periodic risk checks."""
    print(f"[DailyRoutine] Running daily routine at {datetime.now()}")
    trade_logic()
    # schedule periodic risk checks while market is open:
    schedule.every(MANAGE_RISK_INTERVAL_MINUTES).minutes.do(manage_risk)
    print("[DailyRoutine] Scheduled periodic risk checks every", MANAGE_RISK_INTERVAL_MINUTES, "minutes.")

def schedule_jobs():
    # refresh universe shortly after open, then run trades
    schedule.every().day.at(DAILY_REFRESH_TIME).do(lambda: (print("[Schedule] Refreshing universe..."), get_universe()))
    schedule.every().day.at(DAILY_TRADE_TIME).do(daily_routine)
    # clear daily purchased_options once a day (choose a time after market close)
    schedule.every().day.at("20:00").do(reset_daily_state)  # adjust timezone as needed

# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print("🚀 Dynamic Options Swing (Alpaca Native) started.", datetime.now())
    schedule_jobs()
    # immediate run at startup (safe)
    try:
        trade_logic()
        manage_risk()
    except Exception as e:
        print(f"[StartupError] {e}")

    # scheduler main loop
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print(f"[Scheduler Error] {e}")
            time.sleep(5)

