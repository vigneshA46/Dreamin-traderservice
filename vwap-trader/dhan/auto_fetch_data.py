import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from io import StringIO


# =========================
# CONFIG
# =========================

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY4MDI2MTYyLCJpYXQiOjE3Njc5Mzk3NjIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.kn-oxtkC9BEsyJOmtQ1oC46XvdgopoVd4S0CwfRqI7b1HBXayAcwDJKirC7Ey44M8smkJnCGW8XH63vmFAHzbw"

INSTRUMENT_URL = "https://api.dhan.co/v2/instrument/NSE_FNO"
HIST_URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

IST = pytz.timezone("Asia/Kolkata")

FROM_TIME = "09:15:00"
TO_TIME   = "15:30:00"
INTERVAL  = "1"  # 1-minute candles

TARGET_SYMBOL = "NIFTY"

# =========================
# STEP 1: LOAD FNO MASTER
# =========================

def load_fno_master():
    print("📥 Downloading FNO instrument master...")

    headers = {
        "access-token": ACCESS_TOKEN
    }

    r = requests.get(INSTRUMENT_URL, headers=headers)
    r.raise_for_status()

    df = pd.read_csv(
        StringIO(r.text),
        header=None,
        low_memory=False
    )

    print("Columns received:", df.columns)
    print("heads received:", df.head)

    df.columns = [
        "EXCH_ID","SEGMENT","SECURITY_ID","ISIN","INSTRUMENT",
        "UNDERLYING_SECURITY_ID","UNDERLYING_SYMBOL","SYMBOL_NAME",
        "DISPLAY_NAME","INSTRUMENT_TYPE","SERIES","LOT_SIZE",
        "SM_EXPIRY_DATE","STRIKE_PRICE","OPTION_TYPE","TICK_SIZE",
        "EXPIRY_FLAG","BRACKET_FLAG","COVER_FLAG","ASM_GSM_FLAG",
        "ASM_GSM_CATEGORY","BUY_SELL_INDICATOR",
        "BUY_CO_MIN_MARGIN_PER","BUY_CO_SL_RANGE_MAX_PERC",
        "BUY_CO_SL_RANGE_MIN_PERC","BUY_BO_MIN_MARGIN_PER",
        "BUY_BO_PROFIT_RANGE_MAX_PERC","BUY_BO_PROFIT_RANGE_MIN_PERC",
        "MTF_LEVERAGE","RESERVED"
    ]

    return df

# =========================
# STEP 2: GET NEAREST FUT
# =========================

def get_nearest_nifty_fut(df, trade_date):
    futs = df[
        (df["INSTRUMENT"] == "FUTIDX") &
        (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL)
    ].copy()


    futs["SM_EXPIRY_DATE"] = pd.to_datetime(futs["SM_EXPIRY_DATE"])
    futs = futs[futs["SM_EXPIRY_DATE"] >= trade_date]

    fut = futs.sort_values("SM_EXPIRY_DATE").iloc[0]
    print(fut)
    return fut


# =========================
# STEP 3: HISTORICAL FETCH
# =========================

def fetch_intraday(security_id, exchange, instrument, from_dt, to_dt, oi=True):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange,
        "instrument": instrument,
        "interval": INTERVAL,
        "oi": oi,
        "fromDate": from_dt,
        "toDate": to_dt
    }

    r = requests.post(HIST_URL, headers=HEADERS, json=payload)
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "oi": data.get("open_interest", [None] * len(data["timestamp"]))
    })

        # ✅ Correct datetime handling
    dt_index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt_index.dt.tz_convert("Asia/Kolkata")

    return df


# =========================
# STEP 4: ATM & ITM LOGIC
# =========================

def calculate_strikes(fut_price, step=50):
    atm = round(fut_price / step) * step
    ce_strike = atm - 2 * step
    pe_strike = atm + 2 * step
    return atm, ce_strike, pe_strike


# =========================
# STEP 5: FIND CE & PE
# =========================

def find_option(df, strike, opt_type, trade_date):
    # Ensure types are correct
    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    strike = float(strike)
    trade_date = pd.to_datetime(trade_date)

    # Filter options
    opt = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL) &
        (df["STRIKE_PRICE"] == strike) &
        (df["OPTION_TYPE"] == opt_type) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ].copy()

    if opt.empty:
        print(f"⚠️ No {opt_type} option found for strike {strike} after {trade_date.date()}")
        print("Available strikes for this option type:")
        available = df[
            (df["INSTRUMENT"] == "OPTIDX") &
            (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL) &
            (df["OPTION_TYPE"] == opt_type)
        ]
        print(available[["STRIKE_PRICE", "SM_EXPIRY_DATE"]])
        return None

    # Return nearest expiry
    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]


# =========================
# MAIN
# =========================

if __name__ == "__main__":

    TRADE_DATE = "2026-01-03"
    trade_date = pd.to_datetime(TRADE_DATE)

    from_dt = f"{TRADE_DATE} {FROM_TIME}"
    to_dt   = f"{TRADE_DATE} {TO_TIME}"

    fno_df = load_fno_master()

    # ---- FUT ----
    fut = get_nearest_nifty_fut(fno_df, trade_date)
    fut_df = fetch_intraday(
        fut["SECURITY_ID"],
        "NSE_FNO",
        "FUTIDX",
        from_dt,
        to_dt
    )
    fut_df["symbol"] = "NIFTY_FUT"
    fut_df["instrument"] = "FUTIDX"
    fut_df["expiry"] = fut["SM_EXPIRY_DATE"]
    fut_df["strike"] = None
    fut_df["option_type"] = None

    # ---- ATM ----
    ref_price = fut_df.iloc[0]["close"]
    atm, ce_strike, pe_strike = calculate_strikes(ref_price)

    print(f"📌 FUT Ref Price: {ref_price}")
    print(f"📌 ATM: {atm}, 2ITM CE: {ce_strike}, 2ITM PE: {pe_strike}")

    # ---- CE ----
    ce = find_option(fno_df, ce_strike, "CE", trade_date)
    ce_df = fetch_intraday(
        ce["SECURITY_ID"],
        "NSE_FNO",
        "OPTIDX",
        from_dt,
        to_dt
    )
    ce_df["symbol"] = f"NIFTY_{ce_strike}_CE"
    ce_df["instrument"] = "OPTIDX"
    ce_df["expiry"] = ce["SM_EXPIRY_DATE"]
    ce_df["strike"] = ce_strike
    ce_df["option_type"] = "CE"

    # ---- PE ----
    pe = find_option(fno_df, pe_strike, "PE", trade_date)
    pe_df = fetch_intraday(
        pe["SECURITY_ID"],
        "NSE_FNO",
        "OPTIDX",
        from_dt,
        to_dt
    )
    pe_df["symbol"] = f"NIFTY_{pe_strike}_PE"
    pe_df["instrument"] = "OPTIDX"
    pe_df["expiry"] = pe["SM_EXPIRY_DATE"]
    pe_df["strike"] = pe_strike
    pe_df["option_type"] = "PE"

    # ---- COMBINE ----
    final_df = pd.concat([fut_df, ce_df, pe_df], ignore_index=True)
    final_df.sort_values(["datetime", "symbol"], inplace=True)
    # Ensure correct dtypes (important)
    final_df["datetime"] = pd.to_datetime(final_df["datetime"])
    final_df["expiry"] = pd.to_datetime(final_df["expiry"], errors="coerce")
    final_df["strike"] = pd.to_numeric(final_df["strike"], errors="coerce")

    # Sort for sanity (optional but recommended)
    final_df = final_df.sort_values(["datetime", "instrument", "option_type"])

    trade_date_str = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    file_name = f"nifty_{trade_date_str}.parquet"

    # Save
    final_df.to_parquet(file_name, index=False)

    print(f"✅ Parquet saved successfully: {file_name}")
    print(final_df.columns)

    print("\n✅ FINAL DATA SAMPLE")
    print(final_df.head(10))
    print(final_df.tail(10))
 