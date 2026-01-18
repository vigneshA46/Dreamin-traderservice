import requests
import pandas as pd
import pytz
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
import os

# =========================
# CONFIG
# =========================

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

IDX_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
FNO_MASTER_URL   = "https://api.dhan.co/v2/instrument/NSE_FNO"

IST = pytz.timezone("Asia/Kolkata")

TRADE_START = "09:15:00"
TRADE_END   = "15:30:00"

NIFTY_INDEX_SECURITY_ID = "13"
TARGET_SYMBOL = "NIFTY"
INTERVAL = "1"  # 1-minute

# =========================
# STEP 1: FETCH INDEX DATA
# =========================

def fetch_index_intraday(trade_date: str):
    payload = {
        "securityId": NIFTY_INDEX_SECURITY_ID,
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": INTERVAL,
        "fromDate": f"{trade_date} {TRADE_START}",
        "toDate": f"{trade_date} {TRADE_END}"
    }

    r = requests.post(IDX_INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()


    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"]
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

# =========================
# STEP 2: SECOND CANDLE
# =========================

def get_second_candle(df: pd.DataFrame):
    candle = df[
        df["datetime"].dt.strftime("%H:%M:%S") == "09:16:00"
    ]

    if candle.empty:
        raise ValueError("❌ Second candle (09:16) not found")

    return candle.iloc[0]

# =========================
# STEP 3: ATM CALCULATION
# =========================

def calculate_atm(index_price: float, step: int = 50):
    return round(index_price / step) * step

# =========================
# STEP 4: LOAD FNO MASTER
# =========================

def load_fno_master():
    r = requests.get(FNO_MASTER_URL, headers={"access-token": ACCESS_TOKEN})
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text), header=None, low_memory=False)

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

    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    return df

# =========================
# STEP 5: FIND CE / PE
# =========================

def find_option_security(df, strike, option_type, trade_date):
    trade_date = pd.to_datetime(trade_date)

    opt = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL) &
        (df["STRIKE_PRICE"] == strike) &
        (df["OPTION_TYPE"] == option_type) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]

# =========================
# STEP 6: FETCH OPTION DATA
# =========================

def fetch_option_intraday(security_id, trade_date):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "interval": INTERVAL,
        "fromDate": f"{trade_date} {TRADE_START}",
        "toDate": f"{trade_date} {TRADE_END}"
    }

    r = requests.post(IDX_INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data.get("volume", [])
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

# =========================
# MAIN
# =========================

if __name__ == "__main__":

    TRADE_DATE = "2026-01-07"

    print("📥 Fetching index data...")
    index_df = fetch_index_intraday(TRADE_DATE)

    second_candle = get_second_candle(index_df)
    index_close = second_candle["close"]
    atm_strike = calculate_atm(index_close)

    print(f"📌 Second candle close: {index_close}")
    print(f"📌 ATM Strike: {atm_strike}")

    fno_df = load_fno_master()

    ce = find_option_security(fno_df, atm_strike, "CE", TRADE_DATE)
    pe = find_option_security(fno_df, atm_strike, "PE", TRADE_DATE)

    print(f"📌 CE Security ID: {ce['SECURITY_ID']}")
    print(f"📌 PE Security ID: {pe['SECURITY_ID']}")

    ce_df = fetch_option_intraday(ce["SECURITY_ID"], TRADE_DATE)
    pe_df = fetch_option_intraday(pe["SECURITY_ID"], TRADE_DATE)

    # =========================
    # ADD METADATA
    # =========================

    for df, inst, opt in [
        (index_df, "INDEX", None),
        (ce_df, "OPTION", "CE"),
        (pe_df, "OPTION", "PE"),
    ]:
        df["instrument"] = inst
        df["option_type"] = opt
        df["strike"] = None if inst == "INDEX" else atm_strike
        df["trade_date"] = TRADE_DATE
        df["atm_strike"] = atm_strike
        df["marked_price"] = index_close

    # =========================
    # COMBINE & SORT
    # =========================

    final_df = pd.concat([index_df, ce_df, pe_df], ignore_index=True)

    final_df["datetime"] = pd.to_datetime(final_df["datetime"])
    final_df = final_df.sort_values(
        ["datetime", "instrument", "option_type"],
        na_position="last"
    ).reset_index(drop=True)

    # =========================
    # SAVE PARQUET
    # =========================

    file_name = f"nifty_option_buying_{TRADE_DATE}.parquet"
    final_df.to_parquet(file_name, index=False)

    print(f"\n✅ Parquet saved successfully: {file_name}")
    print("Rows:", len(final_df))
    print("Columns:", list(final_df.columns))
    print("\n📌 SAMPLE DATA")
    print(final_df.head(5))
 