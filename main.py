import os
import time
import schedule
import yfinance as yf
from alpaca_trade_api import REST, TimeFrame
from datetime import datetime, timedelta

# === CONFIG ===
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

# Debug print
print(f"DEBUG API KEY: {API_KEY}")
print(f"DEBUG SECRET SET: {API_SECRET is not None}")
print(f"DEBUG BASE URL: {BASE_URL}")

# Connect to Alpaca
api = REST(API_KEY, API_SECRET, BASE_URL)

# Choose a list of good tickers (liquid, tradeable, $3–50 range)
TICKERS = ["AAPL", "AMD", "SOFI", "PLTR", "F", "INTC", "T", "NIO", "XPEV", "LCID"]

# === SIMPLE STRATEGY: Momentum Entry ===
def get_signal(ticker):
    try:
        data = api.get_bars(ticker, TimeFrame.Day, limit=10).df
        if len(data) < 10:
            print(f"Not enough data for {ticker}")
            return None
        data["MA5"] = data["close"].rolling(5).mean()
        if data["close"].iloc[-1] > data["MA5"].iloc[-1]:
            return "BUY"
        else:
            return "SELL"
    except Exception as e:
        print(f"Error getting signal for {ticker}: {e}")
        return None

def trade_logic():
    print("\n=== Running Trade Logic ===")
    for ticker in TICKERS:
        signal = get_signal(ticker)
        if not signal:
            continue
        position = None
        try:
            positions = api.list_positions()
            for p in positions:
                if p.symbol == ticker:
                    position = p
                    break
        except Exception as e:
            print(f"Error checking position: {e}")

        if signal == "BUY" and not position:
            try:
                api.submit_order(
                    symbol=ticker,
                    qty=1,
                    side="buy",
                    type="market",
                    time_in_force="gtc",
                )
                print(f"✅ Bought 1 share of {ticker}")
            except Exception as e:
                print(f"Buy error {ticker}: {e}")

        elif signal == "SELL" and position:
            try:
                api.submit_order(
                    symbol=ticker,
                    qty=position.qty,
                    side="sell",
                    type="market",
                    time_in_force="gtc",
                )
                print(f"🧾 Sold {ticker}")
            except Exception as e:
                print(f"Sell error {ticker}: {e}")

# === SCHEDULE ===
print("Starting bot...")
trade_logic()
schedule.every(30).minutes.do(trade_logic)

while True:
    schedule.run_pending()
    time.sleep(60)


