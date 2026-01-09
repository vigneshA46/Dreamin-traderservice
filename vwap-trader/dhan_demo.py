import requests
import json

# ==== CONFIG ====
BASE_URL = "https://api.dhan.co/v2"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3Nzk3OTAzLCJpYXQiOjE3Njc3MTE1MDMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.h175B7fteEM2tCIhiIOKmm7maxjYqK802P0SQDZxy8SoKBvvYy4fccTVJq5XXnjyB6DUSwv2ftq8Kv3GfaVG2A"   # put your token here
SECURITY_ID = "1333"                      # example id, change to your scrip
EXCHANGE_SEGMENT = "NSE_EQ"
INSTRUMENT = "EQUITY"

headers = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN,
}


def get_daily_historical():
    """Daily candles (OHLCV)."""
    url = f"{BASE_URL}/charts/historical"
    payload = {
        "securityId": SECURITY_ID,
        "exchangeSegment": EXCHANGE_SEGMENT,
        "instrument": INSTRUMENT,
        "expiryCode": 0,
        "oi": False,
        "fromDate": "2024-01-01",
        "toDate": "2024-01-10",    # non-inclusive
    }

    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()
    print("=== DAILY HISTORICAL (first 5 candles) ===")
    for i in range(min(5, len(data["timestamp"]))):
        print(
            i,
            "ts:", data["timestamp"][i],
            "O:", data["open"][i],
            "H:", data["high"][i],
            "L:", data["low"][i],
            "C:", data["close"][i],
            "V:", data["volume"][i],
        )


def get_intraday():
    """Intraday candles (1‑min, OHLCV)."""
    url = f"{BASE_URL}/charts/intraday"
    payload = {
        "securityId": SECURITY_ID,
        "exchangeSegment": EXCHANGE_SEGMENT,
        "instrument": INSTRUMENT,
        "interval": "1",   # 1, 5, 15, 25, 60
        "oi": False,
        "fromDate": "2024-09-11 09:30:00",
        "toDate": "2024-09-11 15:30:00",
    }

    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()
    print("\n=== INTRADAY (first 5 candles) ===")
    for i in range(min(5, len(data["timestamp"]))):
        print(
            i,
            "ts:", data["timestamp"][i],
            "O:", data["open"][i],
            "H:", data["high"][i],
            "L:", data["low"][i],
            "C:", data["close"][i],
            "V:", data["volume"][i],
        )


if __name__ == "__main__":
    get_daily_historical()
    get_intraday()
