import pandas as pd
from backtest import run_backtest

PARQUET_PATH = "nifty_2026-01-01.parquet"


def compute_vwap(df):
    df = df.copy()
    # Ensure sorted
    df = df.sort_values("datetime")
    # Reset VWAP every day (important for backtests)
    df["date"] = df["datetime"].dt.date
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    df["cum_pv"] = (typical_price * df["volume"]).groupby(df["date"]).cumsum()
    df["cum_vol"] = (
        df["volume"]
    ).groupby(df["date"]).cumsum()
    df["vwap"] = df["cum_pv"] / df["cum_vol"]
    return df.drop(columns=["cum_pv", "cum_vol", "date"])


def main():
    # ---------- LOAD DATA ----------
    df = pd.read_parquet(PARQUET_PATH)
    

    # ---------- SPLIT FUTURES ----------
    fut_df = df[df["instrument"] == "FUTIDX"].copy()
    fut_df.sort_values("datetime", inplace=True)

    # Use FIRST candle future price for strike selection (Code2Algo behavior)
    fut_price = fut_df.iloc[0]["close"]
    print(f"\nUsing Futures Price for Strike Selection: {fut_price}")

    # ---------- SPLIT OPTIONS ----------
    opt_df = df[df["instrument"] == "OPTIDX"].copy()
    opt_df.rename(columns={"option_type": "option_type"}, inplace=True)
    opt_df.sort_values(["strike", "option_type", "datetime"], inplace=True)

    # ---------- COMPUTE VWAP PER STRIKE+TYPE ----------
    opt_df = (
        opt_df
        .groupby(["strike", "option_type"], group_keys=False)
        .apply(compute_vwap)
        .reset_index(drop=True)
    )
    


    # ---------- RUN BACKTEST ----------
    trades_df = run_backtest(opt_df, fut_price)
    ce_trades = trades_df[trades_df["type"] == "CE"].reset_index(drop=True)
    pe_trades = trades_df[trades_df["type"] == "PE"].reset_index(drop=True)

    print("\n📘 CE TRADES")
    print(ce_trades)

    print("\n📕 PE TRADES")
    print(pe_trades)
    if not trades_df.empty:
        print("\nDAY MTM:", trades_df["pnl"].sum())
    else:
        print("\nDAY MTM: 0 (No closed trades)")


if __name__ == "__main__":
    main()
