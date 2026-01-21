import pandas as pd

LOT_SIZE = 65
MTM_LIMIT = 3000


def calculate_vwap(df):
    df = df.copy()
    df["pv"] = df["close"] * df["volume"]
    df["cum_pv"] = df["pv"].cumsum()
    df["cum_vol"] = df["volume"].cumsum()
    df["vwap"] = df["cum_pv"] / df["cum_vol"]
    return df


def round_to_50(x):
    return round(x / 50) * 50


def run_vwap_backtest(df):

    df = df.sort_values("datetime")

    # ============================
    # FUT -> ATM SELECTION
    # ============================
    fut = df[df["instrument"] == "FUTIDX"].copy()
    first_candle = fut[fut["datetime"].dt.time.astype(str) == "09:16:00"].iloc[0]

    atm_price = first_candle["close"]
    atm_strike = round_to_50(atm_price)

    ce_strike = atm_strike - 100
    pe_strike = atm_strike + 100

    # ============================
    # OPTION SELECTION
    # ============================
    opts = df[df["instrument"] == "OPTIDX"].copy()

    ce_df = opts[(opts["strike"] == ce_strike) & (opts["option_type"] == "CE")].copy()
    pe_df = opts[(opts["strike"] == pe_strike) & (opts["option_type"] == "PE")].copy()

    ce_df = calculate_vwap(ce_df)
    pe_df = calculate_vwap(pe_df)

    trades = []
    positions = {"CE": None, "PE": None}
    halt_trading = False

    timeline = sorted(set(ce_df["datetime"]).union(set(pe_df["datetime"])))

    for ts in timeline:

        if halt_trading:
            break

        for label, data in [("CE", ce_df), ("PE", pe_df)]:
            row = data[data["datetime"] == ts]
            if row.empty:
                continue

            row = row.iloc[0]
            price = row["close"]
            vwap = row["vwap"]

            pos = positions[label]

            if pos is None and price > vwap and ts.time().strftime("%H:%M") >= "09:16":
                positions[label] = {
                    "type": label,
                    "entry_time": ts,
                    "entry_price": price,
                }

            elif pos is not None and price < vwap:
                pnl = (price - pos["entry_price"]) * LOT_SIZE

                trades.append({
                    "type": label,
                    "entry_time": pos["entry_time"],
                    "entry_price": pos["entry_price"],
                    "exit_time": ts,
                    "exit_price": price,
                    "pnl": round(pnl, 2),
                    "reason": "VWAP Breakdown"
                })

                positions[label] = None

        # MTM
        mtm = 0
        for label, pos in positions.items():
            if pos:
                data = ce_df if label == "CE" else pe_df
                r = data[data["datetime"] == ts]
                if not r.empty:
                    ltp = r.iloc[0]["close"]
                    mtm += (ltp - pos["entry_price"]) * LOT_SIZE

        if abs(mtm) >= MTM_LIMIT:
            for label, pos in positions.items():
                if pos:
                    data = ce_df if label == "CE" else pe_df
                    r = data[data["datetime"] == ts].iloc[0]

                    pnl = (r["close"] - pos["entry_price"]) * LOT_SIZE

                    trades.append({
                        "type": label,
                        "entry_time": pos["entry_time"],
                        "entry_price": pos["entry_price"],
                        "exit_time": ts,
                        "exit_price": r["close"],
                        "pnl": round(pnl, 2),
                        "reason": "Global MTM Hit"
                    })

            halt_trading = True

    return pd.DataFrame(trades)


if __name__ == "__main__":

    PARQUET_FILE = "vwap_range_2026-01-01_2026-01-16.parquet"

    df = pd.read_parquet(PARQUET_FILE)
    df["datetime"] = pd.to_datetime(df["datetime"])

    all_results = []

    trading_days = sorted(df["trade_date"].unique())

    print("\n📅 TRADING DAYS")
    print("====================")
    for d in trading_days:
        print(d)

    print("\n================ BACKTEST START ================\n")

    cumulative_pnl = 0

    for trade_date, day_df in df.groupby("trade_date"):

        print(f"\n🚀 BACKTEST : {trade_date}")
        print("-" * 60)

        res = run_vwap_backtest(day_df)

        if res.empty:
            print("No trades for this day.")
            continue

        res["trade_date"] = trade_date

        day_pnl = res["pnl"].sum()
        cumulative_pnl += day_pnl

        print(res)

        print("\n📊 DAY SUMMARY")
        print(f"Day PNL        : {round(day_pnl, 2)}")
        print(f"Cumulative PNL : {round(cumulative_pnl, 2)}")

        all_results.append(res)

    if not all_results:
        print("\nNo trades found in range.")
        exit()

    final = pd.concat(all_results, ignore_index=True)

    print("\n================ OVERALL SUMMARY ================\n")

    ce_trades = final[final["type"] == "CE"]
    pe_trades = final[final["type"] == "PE"]

    print(f"CE Trades : {len(ce_trades)}")
    print(f"PE Trades : {len(pe_trades)}")
    print(f"TOTAL     : {len(final)}\n")

    print(f"CE PNL    : {round(ce_trades['pnl'].sum(),2)}")
    print(f"PE PNL    : {round(pe_trades['pnl'].sum(),2)}")
    print(f"TOTAL PNL : {round(final['pnl'].sum(),2)}")
