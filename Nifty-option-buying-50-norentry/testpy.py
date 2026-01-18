import requests
import pandas as pd
import pytz
from io import StringIO
from datetime import datetime

# =========================
# CONFIG
# =========================

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY4NzA5OTIyLCJpYXQiOjE3Njg2MjM1MjIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.GFML53mcCT7wR15zaNNwSpN2FYhEq01ODcRz-3Kw9WrNR8_sZO_MtOlg5-IMdgKJm3rSCaH2P2Hw3gMHAXiTbg"

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


def fetch_index_intraday():
    payload = {
        "securityId": NIFTY_INDEX_SECURITY_ID,
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": INTERVAL,
        "fromDate": f"2026-01-07 {TRADE_START}",
        "toDate": f"2026-01-07 {TRADE_END}"
    }

    r = requests.post(IDX_INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()
    print(data)


fetch_index_intraday()