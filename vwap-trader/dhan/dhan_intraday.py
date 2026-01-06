import requests
import pandas as pd

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3Nzc0MTk5LCJpYXQiOjE3Njc2ODc3OTksInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.yFkICcYhmCVuYteh86PFq4KJ32Yg4wmYez66Ox1B4GVCcd4lMvn_zs-n1KIydyGPbXOQWGMTAOXLKyPILHW_SQ"

URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

FROM_DATE = "2025-12-31 09:15:00"
TO_DATE   = "2025-12-31 15:30:00"
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
        "securityId": "40469",
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "expiry": "2026-01-06",
        "strike": 26050,
        "option_type": "CE",
        "oi": True
    },
    "NIFTY_26250_PE": {
        "securityId": "40480",
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "expiry": "2026-01-06",
        "strike": 26250,
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
        "datetime": pd.to_datetime(data["timestamp"], unit="s"),
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
    final_df.to_parquet(
        "nifty_2025_12_31.parquet",
        engine="pyarrow",
        compression="snappy"
    )

    print("✅ Parquet saved: nifty_2025_12_31.parquet")
    print(final_df.head())
 