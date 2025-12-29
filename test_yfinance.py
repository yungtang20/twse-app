import yfinance as yf

print("Testing yfinance for ^TWII...")
try:
    ticker = yf.Ticker("^TWII")
    hist = ticker.history(period="1mo")
    print(f"Got {len(hist)} records")
    if len(hist) > 0:
        print("Sample data:")
        print(hist.tail(3))
except Exception as e:
    print(f"Error: {e}")
