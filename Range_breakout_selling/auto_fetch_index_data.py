import requests
import pandas as pd
from datetime import datetime
import pytz
from io import StringIO
from dotenv import load_dotenv
import os

# =========================
# CONFIG
# =========================

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

BASE_URL = "https://api.dhan.co/v2"
INTRADAY_URL = f"{BASE_URL}/charts/intraday"
FNO_MASTER_URL = f"{BASE_URL}/instrument/NSE_FNO"
LTP_URL = f"{BASE_URL}/marketfeed/ltp"

LTPHEADERS = {
    "Accept":"application/json",
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN,
    "client-id": "1107425275"
}
HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN,
}

IST = pytz.timezone("Asia/Kolkata")

TRADE_DATE = "2026-01-01"
TRADE_START = "09:15:00"
TRADE_END   = "15:30:00"

NIFTY_SECURITY_ID = "13"
STRIKE_GAP = 50


# =========================
# CORE API LAYER
# =========================

def fetch_index_candles(interval: int):
    payload = {
        "securityId": NIFTY_SECURITY_ID,
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": str(interval),
        "fromDate": f"{TRADE_DATE} {TRADE_START}",
        "toDate": f"{TRADE_DATE} {TRADE_END}",
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data.get("volume", []),
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def fetch_option_candles(security_id: str, interval: int):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "interval": str(interval),
        "fromDate": f"{TRADE_DATE} {TRADE_START}",
        "toDate": f"{TRADE_DATE} {TRADE_END}",
    }

    r = requests.post(INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data.get("volume", []),
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def fetch_ltp(security_id: str):
    payload = {
        "NSE_FNO": [int(security_id)]
    }

    r = requests.post(LTP_URL, headers=LTPHEADERS, json=payload)
    r.raise_for_status()
    print(r.json())

    data = r.json()["data"]["NSE_FNO"][str(security_id)]
    return data["last_price"]


def fetch_instruments():
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
    print("result of FNO master ")
    print(df)
    return df


# =========================
# HELPERS
# =========================

def compute_atm(nifty_df):
    second = nifty_df.iloc[7]
    close_price = second["close"]
    atm = round(close_price / STRIKE_GAP) * STRIKE_GAP

    print(f"\n✅ 9:55-10:00 Candle Close : {close_price}")
    print(f"✅ ATM Strike      : {atm}")

    return atm


def pick_itm5(df, atm):
    df = df.copy()

    # remove junk header row
    df = df[df["EXCH_ID"] != "EXCH_ID"]

    # only nifty index options
    df = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == "NIFTY")
    ]

    df = df.sort_values("SM_EXPIRY_DATE")

    if df.empty:
        raise ValueError("❌ No NIFTY OPTIDX rows found in FNO master")

    expiry = df.iloc[0]["SM_EXPIRY_DATE"]
    df = df[df["SM_EXPIRY_DATE"] == expiry]

    ce_strike = atm - 400
    pe_strike = atm + 400

    ce_df = df[(df["OPTION_TYPE"] == "CE") & (df["STRIKE_PRICE"] == ce_strike)]
    pe_df = df[(df["OPTION_TYPE"] == "PE") & (df["STRIKE_PRICE"] == pe_strike)]

    if ce_df.empty:
        raise ValueError(f"❌ No CE found for strike {ce_strike}")

    if pe_df.empty:
        raise ValueError(f"❌ No PE found for strike {pe_strike}")

    ce = ce_df.iloc[0]
    pe = pe_df.iloc[0]

    print(f"🎯 Picked CE Strike: {ce_strike}")
    print(f"🎯 Picked PE Strike: {pe_strike}")

    return ce, pe


def shift_until_250(row, direction, instruments):
    row = row.copy()

    while True:
        ltp = fetch_ltp(row["SECURITY_ID"])
        print(f"🔎 {row['DISPLAY_NAME']} LTP = {ltp}")

        if ltp >= 250:
            return row

        new_strike = row["STRIKE_PRICE"] + direction * STRIKE_GAP

        row = instruments[
            (instruments["STRIKE_PRICE"] == new_strike) &
            (instruments["OPTION_TYPE"] == row["OPTION_TYPE"])
        ].iloc[0]

def normalize_df(df, symbol, instrument_type, security_id, option_type, strike, expiry, interval):
    df = df.copy()

    df["symbol"] = symbol
    df["instrument_type"] = instrument_type
    df["security_id"] = security_id
    df["option_type"] = option_type
    df["strike"] = strike
    df["expiry"] = expiry
    df["interval"] = interval
    df["trade_date"] = TRADE_DATE

    return df[[
        "symbol","instrument_type","security_id","option_type",
        "strike","expiry","interval","trade_date",
        "datetime","timestamp","open","high","low","close","volume"
    ]]

# =========================
# MAIN TEST
# =========================

def main():
    print("\n🚀 FETCH INDEX DATA")
    nifty_5m = fetch_index_candles(5)
    print(nifty_5m.head())

    print("\n nifty 1mn candles")
    nifty_1m=fetch_index_candles(1)

    print(nifty_1m.head())

    atm = compute_atm(nifty_5m)

    print("\n📦 LOAD FNO MASTER")
    instruments = fetch_instruments()

    ce, pe = pick_itm5(instruments, atm)

    #print("\n🔍 SHIFT CE")
    #ce = shift_until_250(ce, -1, instruments)

    #print("\n🔍 SHIFT PE")
    #pe = shift_until_250(pe, 1, instruments)

    print(f"\n✅ FINAL CE : {ce['DISPLAY_NAME']}")
    print(f"✅ FINAL PE : {pe['DISPLAY_NAME']}")

    print("\n📊 FETCH CE DATA")
    ce_1m = fetch_option_candles(ce["SECURITY_ID"], 1)

    print("\n📊 FETCH PE DATA")
    pe_1m = fetch_option_candles(pe["SECURITY_ID"], 1)

    print("\n================ NIFTY 5M ================")
    print(nifty_5m.head())

    print("\n================ NIFTY 1M ================")
    print(nifty_1m.head())

    print("\n================ CE 1M ===================")
    print(ce_1m.head())

    print("\n================ PE 1M ===================")
    print(pe_1m.head())
    # ================= PARQUET STORE =================

    nifty_5norm = normalize_df(
        nifty_5m, "NIFTY", "INDEX", NIFTY_SECURITY_ID,
        None, None, None, 5
    )
    nifty_1norm = normalize_df(
        nifty_1m, "NIFTY", "INDEX", NIFTY_SECURITY_ID,
        None, None, None, 1
    )


    ce1_norm = normalize_df(
        ce_1m, ce["DISPLAY_NAME"], "OPTION", ce["SECURITY_ID"],
        "CE", ce["STRIKE_PRICE"], ce["SM_EXPIRY_DATE"], 1
    )

    pe1_norm = normalize_df(
        pe_1m, pe["DISPLAY_NAME"], "OPTION", pe["SECURITY_ID"],
        "PE", pe["STRIKE_PRICE"], pe["SM_EXPIRY_DATE"], 1
    )

    final_df = pd.concat([nifty_5norm,nifty_1norm, ce1_norm, pe1_norm])
    print("final df",final_df)
    file_name = f"range_breakout_index_{TRADE_DATE}.parquet"
    final_df.to_parquet(file_name, index=False)

    print(f"\n💾 PARQUET SAVED → {file_name}")


if __name__ == "__main__":
    main()
