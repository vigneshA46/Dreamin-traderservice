import websocket
import json
import struct
import threading
import time
from datetime import datetime
import requests
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv

# ============================
# CONFIG
# ============================

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

BASE_URL = "https://api.dhan.co/v2"
FNO_MASTER_URL = f"{BASE_URL}/instrument/NSE_FNO"

LOT_SIZE = 65
NIFTY_INDEX_ID = "13"

# ============================
# STRATEGY STATE
# ============================

index_ticks = []
index_5m = []

top_line = None
bottom_line = None
atm = None

ce_token = None
pe_token = None

ce_pos = None
pe_pos = None

total_mtm = 0

ws_app = None

# ============================
# UTIL
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
    df = df[(df["INSTRUMENT"] == "OPTIDX") & (df["UNDERLYING_SYMBOL"] == "NIFTY")]
    df = df.sort_values("SM_EXPIRY_DATE")

    expiry = df.iloc[0]["SM_EXPIRY_DATE"]
    df = df[df["SM_EXPIRY_DATE"] == expiry]

    ce = df[(df["OPTION_TYPE"] == "CE") & (df["STRIKE_PRICE"] == atm - 400)].iloc[0]
    pe = df[(df["OPTION_TYPE"] == "PE") & (df["STRIKE_PRICE"] == atm + 400)].iloc[0]

    print(f"🎯 CE TOKEN {ce['SECURITY_ID']}")
    print(f"🎯 PE TOKEN {pe['SECURITY_ID']}")

    return str(ce["SECURITY_ID"]), str(pe["SECURITY_ID"])


# ============================
# BINARY PARSER
# ============================

def parse_binary_packet(message: bytes):
    """
    Parses only Ticker packets (ResponseCode = 2)
    """

    if len(message) < 16:
        return None

    # Header (little endian)
    response_code = message[0]
    msg_len = struct.unpack("<H", message[1:3])[0]
    exchange_segment = message[3]
    security_id = struct.unpack("<I", message[4:8])[0]

    # Ticker packet
    if response_code == 2:
        ltp = struct.unpack("<f", message[8:12])[0]
        ltt = struct.unpack("<I", message[12:16])[0]

        return {
            "type": "ticker",
            "security_id": str(security_id),
            "ltp": ltp,
            "ltt": ltt
        }

    return None


# ============================
# STRATEGY CORE
# ============================

def on_tick(tick):
    global index_ticks, index_5m
    global top_line, bottom_line, atm
    global ce_token, pe_token

    token = tick["security_id"]
    price = tick["ltp"]
    now = datetime.now()

    # ================= INDEX =================

    if token == NIFTY_INDEX_ID:
        index_ticks.append(price)

        if len(index_ticks) == 5:
            candle = {
                "open": index_ticks[0],
                "high": max(index_ticks),
                "low": min(index_ticks),
                "close": index_ticks[-1],
                "time": now
            }

            index_5m.append(candle)
            index_ticks.clear()

            print("📊 5M:", candle)

            if len(index_5m) == 2 and top_line is None:
                setup = index_5m[-1]

                top_line = max(setup["open"], setup["close"])
                bottom_line = min(setup["open"], setup["close"])
                atm = round(setup["close"] / 50) * 50

                print("\n================ SETUP ================")
                print("TOP    :", top_line)
                print("BOTTOM :", bottom_line)
                print("ATM    :", atm)

                instruments = fetch_instruments()
                ce_token, pe_token = pick_itm5(instruments, atm)

                subscribe_symbols([
                    {"ExchangeSegment": "NSE_FNO", "SecurityId": ce_token},
                    {"ExchangeSegment": "NSE_FNO", "SecurityId": pe_token},
                ])

    # ================= OPTIONS =================

    elif token in [ce_token, pe_token]:
        print(f"📈 OPT {token} -> {price}")


# ============================
# WSS HANDLERS
# ============================

def on_message(ws, message):
    if isinstance(message, bytes):
        tick = parse_binary_packet(message)
        if tick:
            on_tick(tick)


def on_error(ws, error):
    print("❌ WSS ERROR:", error)


def on_close(ws):
    print("⚠ WSS CLOSED... reconnecting")
    time.sleep(3)
    start_ws()


def on_open(ws):
    print("✅ WSS CONNECTED")

    subscribe_symbols([
        {"ExchangeSegment": "NSE", "SecurityId": NIFTY_INDEX_ID}
    ])


# ============================
# SUBSCRIBE
# ============================

def subscribe_symbols(instruments):
    payload = {
        "RequestCode": 15,
        "InstrumentCount": len(instruments),
        "InstrumentList": instruments
    }

    ws_app.send(json.dumps(payload))
    print("📡 SUBSCRIBED:", instruments)


# ============================
# STARTER
# ============================

def start_ws():
    global ws_app

    url = (
        f"wss://api-feed.dhan.co?"
        f"version=2&token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2"
    )

    ws_app = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    
    threading.Thread(target=ws_app.run_forever, daemon=True).start()


# ============================
# RUN
# ============================

if __name__ == "__main__":
    print("🚀 Range Breakout Dhan WSS Started")
    start_ws()

    while True:
        time.sleep(1)
 