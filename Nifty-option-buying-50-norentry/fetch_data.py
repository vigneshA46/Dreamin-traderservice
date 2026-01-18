import requests
import pandas as pd

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY4MjIxNzYxLCJpYXQiOjE3NjgxMzUzNjEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.-aELpvMAUMDELCQdCU-9EhupVKyTtrriUwvwMmkzMq0V8YS5uBqXLKxr_PQ49omtYOj-Ef9jCLHXlSclERBYDg"

INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

def fetch_intraday_data(
    security_id: str,
    exchange_segment: str,
    instrument: str,
    from_date: str,
    to_date: str,
    expiry_code: int = 0,
    oi: bool = False
):
    payload = {
        "securityId": security_id,
        "exchangeSegment": exchange_segment,
        "instrument": instrument,
        "expiryCode": 0,
        "oi": oi,
        "fromDate": from_date,
        "toDate": to_date
    }

    response = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    response.raise_for_status()

    data = response.json()

    df = pd.DataFrame({
        "timestamp": pd.to_datetime(data["timestamp"], unit="s"),
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data.get("volume", [])
    })
    df["timestamp"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")

    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


if __name__ == "__main__":

    # ✅ NIFTY 50 INDEX (confirmed)
    SECURITY_ID = "13"
    EXCHANGE_SEGMENT = "IDX_I"
    INSTRUMENT = "INDEX"
    FROM_DATE = "2025-01-01"
    TO_DATE = "2025-01-01"

    df = fetch_intraday_data(
        security_id=SECURITY_ID,
        exchange_segment=EXCHANGE_SEGMENT,
        instrument=INSTRUMENT,
        from_date=FROM_DATE,
        to_date=TO_DATE
    )

    print(df.head(10))
    print("\nLast candle:")
    print(df.tail(1))
    print("\nTotal candles:", len(df))
      # 🔹 Extract 9:15–9:16 candle (IST)
    candle_916 = df[
        (df["timestamp"].dt.hour == 9) &
        (df["timestamp"].dt.minute == 16)
    ]

    if candle_916.empty:
        print("\n❌ 9:15–9:16 candle not found")
    else:
        close_916 = candle_916.iloc[0]["close"]
        atm_strike = round(close_916 / 50) * 50

        print("\n📌 9:15–9:16 Candle Close (Index):", close_916)
        print("📌 Calculated ATM Strike:", atm_strike)
