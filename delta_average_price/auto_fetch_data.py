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

INTERVAL = "1"  # 1-minute

# NIFTY INDEX CONFIG
NIFTY_INDEX_SECURITY_ID = "13"
INDEX_EXCHANGE_SEGMENT  = "IDX_I"

# STRATEGY CONFIG
TARGET_SYMBOL  = "NIFTY"
STRIKE_STEP    = 50
STRIKE_OFFSET  = 250   # CE = ATM - 200 | PE = ATM + 200

# =====================================================
# STEP 1: FETCH INDEX INTRADAY DATA
# =====================================================

def fetch_index_intraday(trade_date: str) -> pd.DataFrame:
    payload = {
        "securityId": NIFTY_INDEX_SECURITY_ID,
        "exchangeSegment": INDEX_EXCHANGE_SEGMENT,
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

# =====================================================
# STEP 2: GET SECOND 1-MIN CANDLE (09:16 CLOSE)
# =====================================================

def get_second_candle_close(df: pd.DataFrame) -> float:
    candle = df[df["datetime"].dt.strftime("%H:%M:%S") == "09:16:00"]

    if candle.empty:
        raise ValueError("❌ 09:16 candle not found")

    return candle.iloc[0]["close"]

# =====================================================
# STEP 3: ATM CALCULATION
# =====================================================

def calculate_atm(index_price: float) -> int:
    return round(index_price / STRIKE_STEP) * STRIKE_STEP

# =====================================================
# STEP 4: LOAD FNO MASTER
# =====================================================

def load_fno_master() -> pd.DataFrame:
    print("...downloading FNO master")
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

# =====================================================
# STEP 5: FIND OPTION SECURITY ID
# =====================================================

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

# =====================================================
# STEP 6: FETCH OPTION INTRADAY DATA
# =====================================================

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

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":

    TRADE_DATE = "2026-01-13"

    print("📥 Fetching NIFTY index data...")
    index_df = fetch_index_intraday(TRADE_DATE)

    index_close_0916 = get_second_candle_close(index_df)
    atm_strike = calculate_atm(index_close_0916)

    ce_strike = atm_strike - STRIKE_OFFSET
    pe_strike = atm_strike + STRIKE_OFFSET

    print(f"📌 09:16 Close : {index_close_0916}")
    print(f"📌 ATM Strike : {atm_strike}")
    print(f"📌 CE Strike  : {ce_strike}")
    print(f"📌 PE Strike  : {pe_strike}")

    fno_df = load_fno_master()

    ce_info = find_option_security(fno_df, ce_strike, "CE", TRADE_DATE)
    pe_info = find_option_security(fno_df, pe_strike, "PE", TRADE_DATE)
    ce_expiry = ce_info["SM_EXPIRY_DATE"].date()
    pe_expiry = pe_info["SM_EXPIRY_DATE"].date()
    print(f"📌 CE Expiry  : {ce_expiry}")
    print(f"📌 PE Expiry  : {pe_expiry}")


    ce_df = fetch_option_intraday(ce_info["SECURITY_ID"], TRADE_DATE)
    pe_df = fetch_option_intraday(pe_info["SECURITY_ID"], TRADE_DATE)

    # =================================================
    # METADATA
    # =================================================

    index_df["instrument"] = "INDEX"
    index_df["option_type"] = None
    index_df["strike"] = None

    ce_df["instrument"] = "OPTION"
    ce_df["option_type"] = "CE"
    ce_df["strike"] = ce_strike
    ce_df["expiry"] = ce_expiry
    pe_df["expiry"] = pe_expiry
    index_df["expiry"] = None

    pe_df["instrument"] = "OPTION"
    pe_df["option_type"] = "PE"
    pe_df["strike"] = pe_strike

    for df in [index_df, ce_df, pe_df]:
        df["trade_date"] = TRADE_DATE
        df["atm_strike"] = atm_strike
        df["marked_price"] = index_close_0916
        df["strike_offset"] = STRIKE_OFFSET
        df["strategy"] = "NIFTY ITM Option Buying with TSL"

    final_df = pd.concat([index_df, ce_df, pe_df], ignore_index=True)

    final_df = final_df.sort_values(
        ["datetime", "instrument", "option_type"],
        na_position="last"
    ).reset_index(drop=True)

    file_name = f"nifty_itm_option_strategy_{TRADE_DATE}.parquet"
    final_df.to_parquet(file_name, index=False)

    print(f"\n✅ Parquet saved: {file_name}")
    print("Rows:", len(final_df))
    print("Columns:", list(final_df.columns))
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    print("first 10 rows")
    print(final_df.head(10))
 