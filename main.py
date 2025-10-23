import os
import time
import schedule
import yfinance as yf
from alpaca_trade_api.rest import REST

# === ALPACA SETUP ===
API_KEY = os.environ.get("ALPACA_API_KEY")
API_SECRET = os.environ.get("ALPACA_SECRET_KEY")
BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
api = REST(API_KEY, API_SECRET, BASE_URL)

# === SETTINGS ===
RISK_PER_TRADE = 0.02       # 2% of portfolio per trade
STOP_LOSS_PCT = 0.35        # 35% stop-loss
TOP_N_STOCKS = 10           # number of stocks to trade
OPTION_DAYS = 21            # near-term options
MIN_PRICE = 3
MAX_PRICE = 50
MIN_VOLUME = 1_000_000      # min daily volume
MIN_OPTION_PRICE = 0.5      # min last price of option
MIN_OPTION_VOLUME = 100     # min contracts traded

# === GLOBALS ===
symbols = []  # dynamically selected symbols
purchased_options = set()  # track already bought options today

# === FUNCTIONS ===

def get_cash():
    account = api.get_account()
    return float(account.cash)

def get_position(symbol):
    try:
        return api.get_position(symbol)
    except:
        return None

def select_symbols():
    """
    Dynamically select top N stocks by dollar volume between $3 and $50.
    """
    global symbols
    # For demo purposes, using fixed watchlist
    candidate_list = ["AAPL", "TSLA", "MSFT", "NVDA", "AMD", "COIN", "META", "BA", "NFLX", "F"]
    symbols = []

    for sym in candidate_list:
        try:
            ticker = yf.Ticker(sym)
            info = ticker.info
            price = info.get('regularMarketPrice', None)
            volume = info.get('averageDailyVolume10Day', None)
            if price and MIN_PRICE <= price <= MAX_PRICE and volume and volume > MIN_VOLUME:
                symbols.append({
                    "symbol": sym,
                    "dollar_volume": price * volume
                })
        except:
            continue

    symbols_sorted = sorted(symbols, key=lambda x: x['dollar_volume'], reverse=True)[:TOP_N_STOCKS]
    symbols[:] = [s['symbol'] for s in symbols_sorted]
    print(f"[Symbol Selection] Selected: {symbols}")

def select_options(symbol):
    """
    Get ATM call and put options for a symbol using yfinance, filtered by price & volume.
    """
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None, None
        exp = expirations[0]  # nearest expiration
        chain = ticker.option_chain(exp)
        calls = chain.calls
        puts  = chain.puts
        underlying_price = ticker.history(period="1d")['Close'][-1]

        # Filter by minimum price and volume
        calls = calls[(calls['lastPrice'] >= MIN_OPTION_PRICE) & (calls['volume'] >= MIN_OPTION_VOLUME)]
        puts  = puts[(puts['lastPrice'] >= MIN_OPTION_PRICE) & (puts['volume'] >= MIN_OPTION_VOLUME)]
        if calls.empty or puts.empty:
            return None, None

        atm_call = calls.iloc[(calls['strike'] - underlying_price).abs().argsort()[0]]
        atm_put  = puts.iloc[(puts['strike'] - underlying_price).abs().argsort()[0]]
        return atm_call, atm_put
    except Exception as e:
        print(f"[Error] Failed to fetch options for {symbol}: {e}")
        return None, None

def trade_logic():
    cash = get_cash()
    max_invest = cash * RISK_PER_TRADE

    if not symbols:
        select_symbols()

    for symbol in symbols:
        call, put = select_options(symbol)
        if call is None or put is None:
            continue

        for opt in [call, put]:
            contract = opt['contractSymbol']
            if contract in purchased_options:
                continue  # skip if already bought

            qty = int(max_invest / (opt['lastPrice'] * 100))
            if qty < 1:
                continue

            api.submit_order(
                symbol=contract,
                qty=qty,
                side="buy",
                type="market",
                time_in_force="day"
            )
            purchased_options.add(contract)
            print(f"[Trade] Bought {contract} x{qty} at {opt['lastPrice']}")

def manage_risk():
    positions = api.list_positions()
    for pos in positions:
        entry_price = float(pos.avg_entry_price)
        current_price = float(pos.current_price)
        symbol = pos.symbol

        if current_price < entry_price * (1 - STOP_LOSS_PCT):
            api.submit_order(
                symbol=symbol,
                qty=int(pos.qty),
                side="sell",
                type="market",
                time_in_force="day"
            )
            print(f"[Stop-Loss] Sold {symbol} at {current_price:.2f} (entry {entry_price:.2f})")

# === SCHEDULER ===
schedule.every().day.at("09:30").do(select_symbols)  # refresh symbols daily at market open
schedule.every(30).minutes.do(trade_logic)
schedule.every(10).minutes.do(manage_risk)

# Run once immediately
select_symbols()
trade_logic()
manage_risk()

while True:
    schedule.run_pending()
    time.sleep(60)
