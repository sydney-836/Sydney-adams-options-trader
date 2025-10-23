import os
import time
import schedule
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame

# === ALPACA SETUP ===
API_KEY = os.environ.get("ALPACA_API_KEY")
API_SECRET = os.environ.get("ALPACA_SECRET_KEY")
BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
api = REST(API_KEY, API_SECRET, BASE_URL)

# === SETTINGS ===
RISK_PER_TRADE = 0.02  # 2% of portfolio per trade
STOP_LOSS_PCT = 0.35   # 35% stop-loss
SYMBOLS = ["AAPL", "TSLA", "MSFT"]  # Example: replace with your dynamic selection

# === FUNCTIONS ===
def get_cash():
    account = api.get_account()
    return float(account.cash)

def get_position(symbol):
    try:
        return api.get_position(symbol)
    except:
        return None

def trade_logic():
    cash = get_cash()
    max_invest = cash * RISK_PER_TRADE

    for symbol in SYMBOLS:
        # Get last price
        bar = api.get_bars(symbol, TimeFrame.Minute, "2025-10-23T09:30:00-04:00", "2025-10-23T16:00:00-04:00").df
        if bar.empty:
            continue
        last_price = bar['close'][-1]

        # Determine qty
        qty = int(max_invest / last_price)
        if qty < 1:
            continue

        # Check if already have position
        position = get_position(symbol)
        if position:
            continue

        # Submit order
        api.submit_order(
            symbol=symbol,
            qty=qty,
            side="buy",
            type="market",
            time_in_force="day"
        )
        print(f"Bought {qty} shares of {symbol} at approx {last_price}")

def manage_risk():
    positions = api.list_positions()
    for pos in positions:
        symbol = pos.symbol
        entry_price = float(pos.avg_entry_price)
        current_price = float(pos.current_price)

        if current_price < entry_price * (1 - STOP_LOSS_PCT):
            api.submit_order(
                symbol=symbol,
                qty=int(pos.qty),
                side="sell",
                type="market",
                time_in_force="day"
            )
            print(f"Stopped out of {symbol} at {current_price:.2f}")

# === SCHEDULER ===
schedule.every(30).minutes.do(trade_logic)
schedule.every(10).minutes.do(manage_risk)  # Check stop-loss more frequently

# Run once immediately
trade_logic()
manage_risk()

while True:
    schedule.run_pending()
    time.sleep(60)

