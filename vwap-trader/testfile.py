import time
from tvDatafeed import TvDatafeed, Interval

tv = TvDatafeed()

SYMBOL = "NIFTY1!"
EXCHANGE = "NSE"

last_timestamp = None  # to avoid duplicate prints

while True:
    try:
        df = tv.get_hist(
            symbol=SYMBOL,
            exchange=EXCHANGE,
            interval=Interval.in_1_minute,
            n_bars=2  # fetch only latest candles
        )

        if df is None or df.empty:
            print("⚠️ No data received")
            time.sleep(5)
            continue

        latest_candle = df.iloc[-1]

        # avoid printing same candle again
        if last_timestamp != latest_candle.name:
            last_timestamp = latest_candle.name

            print("\n📊 LIVE NIFTY FUTURES")
            print(f"Time   : {latest_candle.name}")
            print(f"Open   : {latest_candle['open']}")
            print(f"High   : {latest_candle['high']}")
            print(f"Low    : {latest_candle['low']}")
            print(f"Close  : {latest_candle['close']}")
            print(f"Volume : {latest_candle['volume']}")

        time.sleep(5)  # poll every 5 seconds

    except Exception as e:
        print("❌ Error:", e)
        time.sleep(5)
