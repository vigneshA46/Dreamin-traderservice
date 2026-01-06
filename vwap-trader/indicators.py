import pandas as pd

def calculate_vwap(df):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    tpv = tp * df["volume"]
    vwap = tpv.cumsum() / df["volume"].cumsum()
    return vwap