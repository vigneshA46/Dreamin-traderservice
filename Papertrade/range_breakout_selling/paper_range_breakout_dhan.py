import time
import json
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, time as dtime
from dhanhq import marketfeed
import os
from dotenv import load_dotenv

load_dotenv()

# ============================
# CONFIG
# ============================

CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

BASE_URL = "https://api.dhan.co/v2"
FNO_MASTER_URL = f"{BASE_URL}/instrument/NSE_FNO"

LOT_SIZE = 65

DAY_TARGET = 77
DAY_STOP = -39

NIFTY_INDEX_ID = "13"

# ============================
# GLOBAL STATE
# ============================

index_5m = []
index_1m = []

top_line = None
bottom_line = None
atm = None

ce = None
pe = None

ce_token = None
pe_token = None

ce_pos = None
pe_pos = None

total_mtm = 0

feed = None

# ============================
# UTIL FUNCTIONS
# ============================

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

    return df

def pick_itm5(df, atm):
    df = df[df["EXCH_ID"] != "EXCH_ID"]

    df = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == "NIFTY")
    ]

    df = df.sort_values("SM_EXPIRY_DATE")
    expiry = df.iloc[0]["SM_EXPIRY_DATE"]

    df = df[df["SM_EXPIRY_DATE"] == expiry]

    ce_strike = atm - 400
    pe_strike = atm + 400

    ce = df[(df["OPTION_TYPE"] == "CE") & (df["STRIKE_PRICE"] == ce_strike)].iloc[0]
    pe = df[(df["OPTION_TYPE"] == "PE") & (df["STRIKE_PRICE"] == pe_strike)].iloc[0]

    print(f"🎯 CE {ce_strike} | TOKEN {ce['SECURITY_ID']}")
    print(f"🎯 PE {pe_strike} | TOKEN {pe['SECURITY_ID']}")

    return ce, pe

# ============================
# STRATEGY CORE
# ============================

def on_index_tick(price, ts):
    global index_1m, index_5m, top_line, bottom_line, atm

    index_1m.append(price)

    if len(index_1m) == 5:
        candle = {
            "open": index_1m[0],
            "high": max(index_1m),
            "low": min(index_1m),
            "close": index_1m[-1],
            "time": ts
        }
        index_5m.append(candle)
        index_1m.clear()

        print("📊 5m Candle:", candle)

        if len(index_5m) == 2 and top_line is None:
            setup = index_5m[-1]
            top_line = max(setup["open"], setup["close"])
            bottom_line = min(setup["open"], setup["close"])
            atm = round(setup["close"] / 50) * 50

            print("\n================ SETUP ================")
            print("TOP    :", top_line)
            print("BOTTOM :", bottom_line)
            print("ATM    :", atm)

            subscribe_options()


def subscribe_options():
    global ce, pe, ce_token, pe_token, feed

    instruments = fetch_instruments()
    ce, pe = pick_itm5(instruments, atm)

    ce_token = str(ce["SECURITY_ID"])
    pe_token = str(pe["SECURITY_ID"])

    feed.subscribe_symbols([
        (marketfeed.NSE_FNO, ce_token, marketfeed.Full),
        (marketfeed.NSE_FNO, pe_token, marketfeed.Full),
    ])

    print("✅ Options Subscribed")


def on_option_tick(token, price):
    global ce_pos, pe_pos, total_mtm

    if token == ce_token:
        manage_position("CE", price)

    if token == pe_token:
        manage_position("PE", price)


def manage_position(side, price):
    global ce_pos, pe_pos, total_mtm

    pos = ce_pos if side == "CE" else pe_pos

    if pos is None:
        pos = {
            "entry": price,
            "best": price,
            "sl": price + 15,
            "trail": price + 30,
            "active": False
        }
        print(f"🚀 PAPER ENTRY {side} @ {price}")
        if side == "CE":
            ce_pos = pos
        else:
            pe_pos = pos
        return

    pos["best"] = min(pos["best"], price)

    if not pos["active"] and price <= pos["entry"] - 30:
        pos["active"] = True

    if pos["active"]:
        new_trail = pos["best"] + 30
        new_sl = new_trail - 15

        if new_trail < pos["trail"]:
            pos["trail"] = new_trail
            pos["sl"] = new_sl

        if price >= pos["sl"]:
            pnl = (pos["entry"] - price) * LOT_SIZE
            total_mtm += pnl
            print(f"🏁 EXIT {side} @ {price} | PNL {pnl}")
            if side == "CE":
                ce_pos = None
            else:
                pe_pos = None


# ============================
# FEED LOOP
# ============================

def start():
    global feed

    print("🚀 Range Breakout Paper Trader Started")

    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        [(marketfeed.NSE, "13", marketfeed.Quote)],
        version="v2"
    )

    while True:
        feed.run_forever()
        data = feed.get_data()

        print(data)

"""         if not data:
            continue

        token = str(data.get("security_id"))
        price = float(data.get("last_traded_price", 0))
        ts = datetime.now()

        if token == NIFTY_INDEX_ID:
            on_index_tick(price, ts)
        else:
            on_option_tick(token, price)

 """
# ============================
# RUN
# ============================

if __name__ == "__main__":
    start()
 