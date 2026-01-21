import pandas as pd

LOT_SIZE = 65
DAY_TARGET = 77
DAY_STOP   = -39


def run_range_breakout(day_df):

    day_df = day_df.sort_values("datetime").reset_index(drop=True)

    nifty_5m = day_df[(day_df["symbol"] == "NIFTY") & (day_df["interval"] == 5)]
    nifty_1m = day_df[(day_df["symbol"] == "NIFTY") & (day_df["interval"] == 1)]
    opt_1m   = day_df[(day_df["instrument_type"] == "OPTION") & (day_df["interval"] == 1)]

    setup = nifty_5m[nifty_5m["datetime"].dt.time == pd.to_datetime("09:55").time()]
    if setup.empty:
        return pd.DataFrame()

    setup = setup.iloc[0]

    top_line    = max(setup["open"], setup["close"])
    bottom_line = min(setup["open"], setup["close"])
    atm_ref     = setup["close"]

    atm = round(atm_ref / 50) * 50
    ce_strike = atm - 400
    pe_strike = atm + 400

    ce_df = opt_1m[(opt_1m["option_type"] == "CE") & (opt_1m["strike"] == ce_strike)]
    pe_df = opt_1m[(opt_1m["option_type"] == "PE") & (opt_1m["strike"] == pe_strike)]

    trades = []
    ce_pos = None
    pe_pos = None
    pending_ce = False
    pending_pe = False
    total_mtm = 0
    stop_trading = False

    for i in range(len(nifty_1m) - 1):

        row = nifty_1m.iloc[i]
        next_row = nifty_1m.iloc[i + 1]

        if stop_trading:
            break

        if row["datetime"].time() < pd.to_datetime("10:01").time():
            continue

        avg_price = (row["open"] + row["high"] + row["low"] + row["close"]) / 4

        if ce_pos is None:
            if row["close"] < bottom_line and avg_price < bottom_line and avg_price < row["close"]:
                pending_ce = True

        if pe_pos is None:
            if row["close"] > top_line and avg_price > top_line and avg_price < row["close"]:
                pending_pe = True

        if pending_ce:
            opt = ce_df[ce_df["datetime"] == next_row["datetime"]]
            if not opt.empty:
                price = opt.iloc[0]["open"]
                ce_pos = {"entry_time": opt.iloc[0]["datetime"], "entry_price": price, "best": price, "sl": price+15, "trail": price+30, "active": False}
            pending_ce = False

        if pending_pe:
            opt = pe_df[pe_df["datetime"] == next_row["datetime"]]
            if not opt.empty:
                price = opt.iloc[0]["open"]
                pe_pos = {"entry_time": opt.iloc[0]["datetime"], "entry_price": price, "best": price, "sl": price+15, "trail": price+30, "active": False}
            pending_pe = False

        if ce_pos:
            opt = ce_df[ce_df["datetime"] == row["datetime"]]
            if not opt.empty:
                price = opt.iloc[0]["close"]
                ce_pos["best"] = min(ce_pos["best"], price)

                if not ce_pos["active"] and price <= ce_pos["entry_price"] - 30:
                    ce_pos["active"] = True

                if ce_pos["active"]:
                    new_trail = ce_pos["best"] + 30
                    new_sl = new_trail - 15

                    if new_trail < ce_pos["trail"]:
                        ce_pos["trail"] = new_trail
                        ce_pos["sl"] = new_sl

                    if price >= ce_pos["sl"] or row["close"] > bottom_line:
                        pnl = ce_pos["entry_price"] - price
                        total_mtm += pnl

                        trades.append(["CE", ce_strike, ce_pos["entry_time"], row["datetime"], ce_pos["entry_price"], price, pnl])
                        ce_pos = None

        if pe_pos:
            opt = pe_df[pe_df["datetime"] == row["datetime"]]
            if not opt.empty:
                price = opt.iloc[0]["close"]
                pe_pos["best"] = min(pe_pos["best"], price)

                if not pe_pos["active"] and price <= pe_pos["entry_price"] - 30:
                    pe_pos["active"] = True

                if pe_pos["active"]:
                    new_trail = pe_pos["best"] + 30
                    new_sl = new_trail - 15

                    if new_trail < pe_pos["trail"]:
                        pe_pos["trail"] = new_trail
                        pe_pos["sl"] = new_sl

                    if price >= pe_pos["sl"] or row["close"] < top_line:
                        pnl = pe_pos["entry_price"] - price
                        total_mtm += pnl

                        trades.append(["PE", pe_strike, pe_pos["entry_time"], row["datetime"], pe_pos["entry_price"], price, pnl])
                        pe_pos = None

        if total_mtm >= DAY_TARGET or total_mtm <= DAY_STOP:
            stop_trading = True

    return pd.DataFrame(trades, columns=[
        "type","strike","entry_time","exit_time","entry_price","exit_price","pnl"
    ])


if __name__ == "__main__":

    PARQUET_FILE = "range_breakout_index_2026-01-01_2026-01-16.parquet"

    df = pd.read_parquet(PARQUET_FILE)
    df["datetime"] = pd.to_datetime(df["datetime"])

    cumulative_pnl = 0
    equity_curve = []
    all_results = []

    print("\n📅 TRADING DAYS")
    for d in sorted(df["trade_date"].unique()):
        print(d)

    for trade_date, day_df in df.groupby("trade_date"):

        print(f"\n🚀 BACKTEST : {trade_date}")
        print("-"*60)

        res = run_range_breakout(day_df)

        if res.empty:
            print("No trades")
            continue

        day_pnl = res["pnl"].sum() * LOT_SIZE
        cumulative_pnl += day_pnl

        print(res)
        print(f"\nDay PNL        : {round(day_pnl,2)}")
        print(f"Cumulative PNL : {round(cumulative_pnl,2)}")

        equity_curve.append({
            "trade_date": trade_date,
            "day_pnl": round(day_pnl,2),
            "cumulative_pnl": round(cumulative_pnl,2)
        })

        res["trade_date"] = trade_date
        all_results.append(res)

    print("\n================ OVERALL SUMMARY ================\n")

    final = pd.concat(all_results, ignore_index=True)

    print("TOTAL TRADES:", len(final))
    print("TOTAL PNL   :", round(final["pnl"].sum() * LOT_SIZE, 2))

    print("\n================ EQUITY CURVE ===================\n")
    print(pd.DataFrame(equity_curve).to_string(index=False))
 