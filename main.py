import os
import time
import schedule
from datetime import datetime
from alpaca_trade_api import REST, TimeFrame

# === CONFIG ===
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

print(f"DEBUG API KEY: {API_KEY}")
print(f"DEBUG SECRET SET: {API_SECRET is not None}")
print(f"DEBUG BASE URL: {BASE_URL}")

# Connect to Alpaca
api = REST(API_KEY, API_SECRET, BASE_URL)

TICKERS = ["AAPL", "AMD", "SOFI", "PLTR", "F", "INTC", "T", "NIO", "XPEV", "LCID"]

# === SIMPLE STRATEGY ===
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


# === TEST TRADE WITH AUTO-SELL ===
def test_trade():
    print("\n=== Running Test Trade ===")
    try:
        # Place test buy
        api.submit_order(
            symbol="AAPL",
            qty=1,
            side="buy",
            type="market",
            time_in_force="gtc",
        )
        print("✅ Test trade successful: Bought 1 share of AAPL.")

        # Wait 60 seconds
        print("⏳ Waiting 60 seconds before auto-sell...")
        time.sleep(60)

        # Auto-sell test trade
        api.submit_order(
            symbol="AAPL",
            qty=1,
            side="sell",
            type="market",
            time_in_force="gtc",
        )
        print("💸 Test trade closed: Sold 1 share of AAPL.")
    except Exception as e:
        print(f"❌ Test trade failed: {e}")


# === MAIN ===
print("🚀 Starting bot...")
test_trade()  # run test trade once
trade_logic()  # run strategy after test trade
schedule.every(30).minutes.do(trade_logic)

while True:
    schedule.run_pending()
    time.sleep(60)
