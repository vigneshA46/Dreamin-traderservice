from dhanhq import marketfeed
import os

client_id = "1107425275"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NTQxMjQ4LCJpYXQiOjE3Njc0NTQ4NDgsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.Z_sltxidOZSXKgoNeMH5XLvI2K1dMGO3EiVlQzB1aeCDLnzP8v-PN8DEoUN9ts6ygQ6ovCOXGMqRlfn-BJbSkQ"

instruments = [
    (marketfeed.NSE_FNO, "49229", marketfeed.Ticker),  # NIFTY FUT
    (marketfeed.NSE_FNO, "49229", marketfeed.Full)
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
