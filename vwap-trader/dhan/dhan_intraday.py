import requests
import pandas as pd

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3ODkyNDIyLCJpYXQiOjE3Njc4MDYwMjIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.-IrvzQ7_mNhukOzo6VpBva_3MFei2yjwqFR6Hfy35kEpvm5oyeevl9WUPuvElvrFeS2wNfkJycvAuLD4jvVAxw"

URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3ODkyNDIyLCJpYXQiOjE3Njc4MDYwMjIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.-IrvzQ7_mNhukOzo6VpBva_3MFei2yjwqFR6Hfy35kEpvm5oyeevl9WUPuvElvrFeS2wNfkJycvAuLD4jvVAxw"
}

FROM_DATE = "2026-1-1 09:15:00"
TO_DATE   = "2026-1-1 15:30:00"
INTERVAL  = "1"

INSTRUMENTS = {
    "NIFTY_FUT": {
        "securityId": "49229",
        "exchangeSegment": "NSE_FNO",
        "instrument": "FUTIDX",
        "expiry": "2025-01-02",
        "strike": None,
        "option_type": None,
        "oi": True
    },
    "NIFTY_26050_CE": {
        "securityId": "40471",
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "expiry": "2026-01-06",
        "strike": 26100,
        "option_type": "CE",
        "oi": True
    },
    "NIFTY_26250_PE": {
        "securityId": "40488",
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "expiry": "2026-01-06",
        "strike": 26300,
        "option_type": "PE",
        "oi": True
    }
}

def fetch_intraday(name, cfg):
    payload = {
        "securityId": cfg["securityId"],
        "exchangeSegment": cfg["exchangeSegment"],
        "instrument": cfg["instrument"],
        "interval": INTERVAL,
        "oi": cfg["oi"],
        "fromDate": FROM_DATE,
        "toDate": TO_DATE
    }

    r = requests.post(URL, headers=HEADERS, json=payload)
    data = r.json()


    df = pd.DataFrame({
        "datetime": pd.to_datetime(
    data["timestamp"], unit="s", utc=True).tz_convert("Asia/Kolkata"),
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "oi": data.get("open_interest", [0]*len(data["timestamp"]))
    })

    # ---- Metadata ----
    df["symbol"] = name
    df["instrument"] = cfg["instrument"]
    df["expiry"] = cfg["expiry"]
    df["strike"] = cfg["strike"]
    df["option_type"] = cfg["option_type"]

    return df


if __name__ == "__main__":
    all_data = []

    for name, cfg in INSTRUMENTS.items():
        df = fetch_intraday(name, cfg)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    # Sort is VERY important
    final_df.sort_values(["datetime", "symbol"], inplace=True)

    # Save parquet
    #final_df.to_parquet(
     #   "nifty_2026_1_1.parquet",
      #  engine="pyarrow",
       # compression="snappy"
    #)

    print("✅ Parquet saved: nifty_2026_1_1.parquet")
    print(final_df)
 