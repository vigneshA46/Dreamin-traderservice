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

INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
MCX_MASTER_URL = "https://api.dhan.co/v2/instrument/MCX_COMM"

IST = pytz.timezone("Asia/Kolkata")

TRADE_START = "09:00:00"
TRADE_END   = "23:30:00"

UNDERLYING_SYMBOL = "CRUDEOIL"
STRIKE_STEP = 50
INTERVAL_1M = "1"
INTERVAL_15M = "15"

# =====================================================
# STEP 1: LOAD MCX MASTER
# =====================================================

def load_mcx_master():
    print("downloading fno master")
    r = requests.get(MCX_MASTER_URL, headers={"access-token": ACCESS_TOKEN})
    if r.status_code != 200:
        print(f"⚠️ No data ")
        return pd.DataFrame()

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
# STEP 2: FIND CURRENT MONTH FUTURE
# =====================================================

def find_current_month_future(df, trade_date):
    trade_date = pd.to_datetime(trade_date)

    fut = df[
        (df["INSTRUMENT"] == "FUTCOM") &
        (df["UNDERLYING_SYMBOL"] == UNDERLYING_SYMBOL) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if fut.empty:
        raise ValueError("❌ No CRUDEOIL future found")

    return fut.sort_values("SM_EXPIRY_DATE").iloc[0]

# =====================================================
# STEP 3: FETCH INTRADAY DATA
# =====================================================

def fetch_intraday(security_id, instrument, interval, trade_date):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "MCX_COMM",
        "instrument": instrument,
        "interval": interval,
        "fromDate": f"{trade_date} {TRADE_START}",
        "toDate": f"{trade_date} {TRADE_END}"
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
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
# STEP 4: GET 3:15–3:30 FUT CANDLE
# =====================================================

def get_315_candle(df):
    candle = df[
        df["datetime"].dt.strftime("%H:%M:%S") == "15:30:00"
    ]

    if candle.empty:
        raise ValueError("❌ 3:15–3:30 candle not found")

    return candle.iloc[0]

# =====================================================
# STEP 5: ATM STRIKE
# =====================================================

def calculate_atm(price):
    return round(price / STRIKE_STEP) * STRIKE_STEP

# =====================================================
# STEP 6: FIND CE / PE
# =====================================================

def find_option(df, strike, option_type, trade_date):
    trade_date = pd.to_datetime(trade_date)

    opt = df[
        (df["INSTRUMENT"] == "OPTFUT") &
        (df["UNDERLYING_SYMBOL"] == UNDERLYING_SYMBOL) &
        (df["STRIKE_PRICE"] == strike) &
        (df["OPTION_TYPE"] == option_type) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":

    TRADE_DATE = "2026-01-14"

    print("📥 Loading MCX instrument master...")
    mcx_df = load_mcx_master()

    print("📥 Finding CRUDEOIL current month future...")
    fut = find_current_month_future(mcx_df, TRADE_DATE)

    print("📥 Fetching FUT 15m data...")
    fut_df = fetch_intraday(
        fut["SECURITY_ID"],
        instrument="FUTCOM",
        interval=INTERVAL_15M,
        trade_date=TRADE_DATE
    )

    candle_315 = get_315_candle(fut_df)
    marked_price = candle_315["close"]
    atm_strike = calculate_atm(marked_price)

    print(f"📌 3:15 Close: {marked_price}")
    print(f"📌 ATM Strike: {atm_strike}")

    ce = find_option(mcx_df, atm_strike, "CE", TRADE_DATE)
    pe = find_option(mcx_df, atm_strike, "PE", TRADE_DATE)
    print(f"📅 CE Expiry: {ce['SM_EXPIRY_DATE'].date()}")
    print(f"📅 PE Expiry: {pe['SM_EXPIRY_DATE'].date()}")

    print(f"📌 CE ID: {ce['SECURITY_ID']}")
    print(f"📌 PE ID: {pe['SECURITY_ID']}")

    ce_df = fetch_intraday(ce["SECURITY_ID"], "OPTFUT", INTERVAL_1M, TRADE_DATE)
    pe_df = fetch_intraday(pe["SECURITY_ID"], "OPTFUT", INTERVAL_1M, TRADE_DATE)

    # =====================================================
    # ADD METADATA
    # =====================================================

    for df, inst, opt, expiry in [
        (fut_df, "FUTURE", None, None),
        (ce_df, "OPTION", "CE", ce["SM_EXPIRY_DATE"]),
        (pe_df, "OPTION", "PE", pe["SM_EXPIRY_DATE"]),
    ]:
        df["instrument"] = inst
        df["option_type"] = opt
        df["strike"] = atm_strike if opt else None
        df["trade_date"] = TRADE_DATE
        df["marked_price"] = marked_price
        df["expiry_date"] = expiry

    final_df = pd.concat([fut_df, ce_df, pe_df], ignore_index=True)
    final_df.sort_values(
        ["datetime", "instrument", "option_type"],
        inplace=True
    )

    file_name = f"crudeoil_intraday_{TRADE_DATE}.parquet"
    final_df.to_parquet(file_name, index=False)

    print(f"\n✅ Parquet saved: {file_name}")
    print("Rows:", len(final_df))
    print("Columns:", list(final_df.columns))
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)
    print("first 10 rows")
    print(final_df.head(10))
    print(final_df.tail(10))