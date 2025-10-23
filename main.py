import os
import time
import schedule
from datetime import datetime, timedelta
from alpaca_trade_api.rest import REST, APIError

# === ALPACA SETUP ===
API_KEY = os.environ.get("ALPACA_API_KEY")
API_SECRET = os.environ.get("ALPACA_SECRET_KEY")
BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
api = REST(API_KEY, API_SECRET, BASE_URL)

# === SETTINGS ===
RISK_PER_TRADE = 0.02        # 2% of account balance per trade
STOP_LOSS_PCT = 0.35         # 35% stop-loss
TOP_N_STOCKS = 10            # number of symbols to trade
MIN_PRICE = 3
MAX_PRICE = 50
MIN_VOLUME = 1_000_000       # min volume for stock selection
MIN_OPTION_VOLUME = 100
MIN_DAYS_TO_EXPIRY = 3       # skip options expiring soon

symbols = []
purchased_options = set()

# === FUNCTIONS ===

def get_cash():
    try:
        account = api.get_account()
        return float(account.cash)
    except Exception as e:
        print(f"[Error] Could not fetch cash balance: {e}")
        return 0.0


def select_symbols():
    """Select top stocks between $3–$50 by volume using Alpaca data."""
    global symbols
    try:
        assets = api.list_assets(status="active")
        tradable = [a for a in assets if a.tradable and a.symbol.isalpha() and len(a.symbol) <= 4]

        prices = []
        for a in tradable:
            try:
                bars = api.get_bars(a.symbol, "1Day", limit=1).df
                if bars.empty:
                    continue
                close = bars["close"].iloc[-1]
                volume = bars["volume"].iloc[-1]
                if MIN_PRICE <= close <= MAX_PRICE and volume > MIN_VOLUME:
                    prices.append((a.symbol, close * volume))
            except Exception:
                continue

        top = sorted(prices, key=lambda x: x[1], reverse=True)[:TOP_N_STOCKS]
        symbols = [s[0] for s in top]
        print(f"[Universe] Selected: {symbols}")

    except Exception as e:
        print(f"[Error] Failed to select symbols: {e}")


def get_option_chain(symbol):
    """Fetch near-term options chain using Alpaca API."""
    try:
        contracts = api.get_options_contracts(symbol)
        if not contracts:
            return []
        valid_contracts = []
        for c in contracts:
            exp_date = datetime.strptime(c.expiration_date, "%Y-%m-%d")
            days_to_expiry = (exp_date - datetime.now()).days
            if days_to_expiry >= MIN_DAYS_TO_EXPIRY:
                valid_contracts.append(c)
        return valid_contracts
    except Exception as e:
        print(f"[Error] Could not fetch option chain for {symbol}: {e}")
        return []


def trade_logic():
    cash = get_cash()
    if cash <= 0:
        print("[Warning] No cash available to trade.")
        return

    max_invest = cash * RISK_PER_TRADE

    if not symbols:
        select_symbols()

    for symbol in symbols:
        contracts = get_option_chain(symbol)
        if not contracts:
            continue

        # Get latest stock price
        try:
            bars = api.get_bars(symbol, "1Day", limit=1).df
            if bars.empty:
                continue
            price = bars["close"].iloc[-1]
        except Exception as e:
            print(f"[Error] Could not get price for {symbol}: {e}")
            continue

        # Filter calls and puts
        calls = [c for c in contracts if c.type == "call"]
        puts = [c for c in contracts if c.type == "put"]

        if not calls or not puts:
            continue

        # Find ATM options
        atm_call = min(calls, key=lambda x: abs(x.strike_price - price))
        atm_put  = min(puts,  key=lambda x: abs(x.strike_price - price))

        for opt in [atm_call, atm_put]:
            if opt.symbol in purchased_options:
                continue

            last_price = opt.underlying_price or price
            if not last_price:
                continue

            qty = int(max_invest / (last_price * 100))
            if qty < 1:
                continue

            try:
                api.submit_order(
                    symbol=opt.symbol,
                    qty=qty,
                    side="buy",
                    type="market",
                    time_in_force="day"
                )
                purchased_options.add(opt.symbol)
                print(f"[Trade] Bought {opt.symbol} x{qty} at approx ${last_price:.2f}")
            except APIError as e:
                print(f"[Error] Could not buy {opt.symbol}: {e}")


def manage_risk():
    """Stop-loss check."""
    try:
        positions = api.list_positions()
        for pos in positions:
            symbol = pos.symbol
            entry = float(pos.avg_entry_price)
            current = float(pos.current_price)
            qty = int(float(pos.qty))

            if current < entry * (1 - STOP_LOSS_PCT):
                try:
                    api.submit_order(
                        symbol=symbol,
                        qty=qty,
                        side="sell",
                        type="market",
                        time_in_force="day"
                    )
                    print(f"[Stop-Loss] Sold {symbol} at ${current:.2f}")
                except APIError as e:
                    print(f"[Error] Could not liquidate {symbol}: {e}")
    except Exception as e:
        print(f"[Error] Risk management failed: {e}")


# === SCHEDULER ===
schedule.every().day.at("09:30").do(select_symbols)
schedule.every(30).minutes.do(trade_logic)
schedule.every(10).minutes.do(manage_risk)

# === MAIN LOOP ===
select_symbols()
trade_logic()
manage_risk()

while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except Exception as e:
        print(f"[Scheduler Error] {e}")
        time.sleep(5)
