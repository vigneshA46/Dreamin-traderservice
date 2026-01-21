from dhanhq import marketfeed
from dotenv import load_dotenv
import os

# =========================
# CONFIG
# =========================

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")


client_id = "1107425275"

instruments = [
    (marketfeed.NSE_FNO, "49229", marketfeed.Ticker),  # NIFTY FUT
    (marketfeed.NSE_FNO, "49229", marketfeed.Full),
]

feed = marketfeed.DhanFeed(
    client_id,
    access_token,
    instruments,
    version="v2"
)

try:
    while True:
        feed.run_forever()
        data = feed.get_data()
        if data:
            print(data)

except KeyboardInterrupt:
    print("Stopped")

finally:
    feed.disconnect()
