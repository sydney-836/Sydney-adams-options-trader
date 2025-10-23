import os
import time
import schedule
import yfinance as yf
from alpaca_trade_api.rest import REST, APIError
from datetime import datetime, timedelta

# === ALPACA SETUP ===
API_KEY = os.environ.get("ALPACA_API_KEY")
API_SECRET = os.environ.get("ALPACA_SECRET_KEY")
BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
api = REST(API_KEY, API_SECRET, BASE_URL)

# === SETTINGS ===
RISK_PER_TRADE = 0.02       # 2% of portfolio per trade
STOP_LOSS_PCT = 0.35        # 35% stop-loss
TOP_N_STOCKS = 10           # number of stocks to trade
MIN_PRICE = 3
MAX_PRICE = 50
MIN_VOLUME = 1_000_000      # min daily volume
MIN_OPTION_PRICE = 0.5      # min last price of option
MIN_OPTION_VOLUME = 100     # min contracts traded
MAX_RETRIES = 3             # max retries for option fetch
MIN_DAYS_TO_EXPIRY = 3      # skip options expiring in <3 days

# === GLOBALS ===
symbols = []                  # dynamically selected symbols
purchased_options = set()     # track already bought options today

# === FUNCTIONS ===
def get_cash():
    account = api.get_account()
    return float(account.cash)

def get_position(symbol):
    try:
        return api.get_position(symbol)
    except:
        return None

def safe_option_fetch(symbol):
    """Fetch ATM call/put options with retries and expiration check."""
    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(symbol)
            expirations = ticker.options
            if not expirations:
                return None, None

            # Filter expirations to skip near-expiry
            valid_exps = [exp for exp in expirations if
                          datetime.strptime(exp, "%Y-%m-%d") - datetime.now() >= timedelta(days=MIN_DAYS_TO_EXPIRY)]
            if not valid_exps:
                return None, None

            exp = valid_exps[0]  # nearest valid expiration
            chain = ticker.option_chain(exp)
            calls = chain.calls
            puts  = chain.puts
            underlying_price = ticker.history(period="1d")['Close'][-1]

            # Filter by price & volume
            calls = calls[(calls['lastPrice'] >= MIN_OPTION_PRICE) & (calls['volume'] >= MIN_OPTION_VOLUME)]
            puts  = puts[(puts['lastPrice'] >= MIN_OPTION_PRICE) & (puts['volume'] >= MIN_OPTION_VOLUME)]
            if calls.empty or puts.empty:
                return None, None

            atm_call = calls.iloc[(calls['strike'] - underlying_price).abs().argsort()[0]]
            atm_put  = puts.iloc[(puts['strike'] - underlying_price).abs().argsort()[0]]
            return atm_call, atm_put
        except Exception as e:
            print(f"[Attempt {attempt+1}] Failed to fetch options for {symbol}: {e}")
            time.sleep(2)
    return None, None

def select_symbols():
    """Dynamically select top N stocks by dollar volume ($3–$50)."""
    global symbols
    candidate_list = ["AAPL","TSLA","MSFT","NVDA","AMD","COIN","META","BA","NFLX","F"]
    symbols = []

    for sym in candidate_list:
        try:
            ticker = yf.Ticker(sym)
            info = ticker.info
            price = info.get('regularMarketPrice', None)
            volume = info.get('averageDailyVolume10Day', None)
            if price and MIN_PRICE <= price <= MAX_PRICE and volume and volume > MIN_VOLUME:
                symbols.append({"symbol": sym, "dollar_volume": price * volume})
        except:
            continue

    symbols_sorted = sorted(symbols, key=lambda x: x['dollar_volume'], reverse=True)[:TOP_N_STOCKS]
    symbols[:] = [s['symbol'] for s in symbols_sorted]
    print(f"[Symbol Selection] Selected: {symbols}")

def trade_logic():
    cash = get_cash()
    max_invest = cash * RISK_PER_TRADE

    if not symbols:
        select_symbols()

    for symbol in symbols:
        call, put = safe_option_fetch(symbol)
        if call is None or put is None:
            continue

        for opt in [call, put]:
            contract = opt['contractSymbol']
            if contract in purchased_options:
                continue

            # Check days to expiration
            exp_date = datetime.strptime(opt['lastTradeDate'], "%Y-%m-%d")
            if (exp_date - datetime.now()).days < MIN_DAYS_TO_EXPIRY:
                print(f"[Skip] {contract} expires in <{MIN_DAYS_TO_EXPIRY} days")
                continue

            qty = int(max_invest / (opt['lastPrice'] * 100))
            if qty < 1:
                continue

            if qty * opt['lastPrice'] * 100 > cash:
                print(f"[Warning] Not enough cash for {qty} of {contract}")
                continue

            try:
                api.submit_order(
                    symbol=contract,
                    qty=qty,
                    side="buy",
                    type="market",
                    time_in_force="day"
                )
                purchased_options.add(contract)
                print(f"[Trade] Bought {contract} x{qty} at {opt['lastPrice']} (exp {exp_date.date()})")
            except Exception as e:
                print(f"[Error] Failed to submit order {contract}: {e}")

def manage_risk():
    positions = api.list_positions()
    for pos in positions:
        entry_price = float(pos.avg_entry_price)
        current_price = float(pos.current_price)
        symbol = pos.symbol

        if current_price < entry_price * (1 - STOP_LOSS_PCT):
            try:
                api.submit_order(
                    symbol=symbol,
                    qty=int(pos.qty),
                    side="sell",
                    type="market",
                    time_in_force="day"
                )
                print(f"[Stop-Loss] Sold {symbol} at {current_price:.2f} (entry {entry_price:.2f})")
            except Exception as e:
                print(f"[Error] Failed to stop-loss {symbol}: {e}")

# === SCHEDULER ===
schedule.every().day.at("09:30").do(select_symbols)  # refresh symbols daily
schedule.every(30).minutes.do(trade_logic)
schedule.every(10).minutes.do(manage_risk)

# Run once immediately
select_symbols()
trade_logic()
manage_risk()

while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except Exception as e:
        print(f"[Error] Scheduler encountered an issue: {e}")
        time.sleep(5)

