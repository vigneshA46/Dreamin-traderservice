import pandas as pd

# ===============================
# CONFIG
# ===============================
BUFFER_POINTS = 7
TARGET_POINTS = 50
TSL_ACTIVATION = 15
TSL_STEP = 10
SL_GAP = 10
MAX_DAILY_LOSS = -2000
LOT_SIZE = 1


# ===============================
# HELPERS
# ===============================
def calc_avg_price(row):
    return (row.open + row.high + row.low + row.close) / 4


# ===============================
# BACKTEST ENGINE
# ===============================
def run_backtest(parquet_file):
    df = pd.read_parquet(parquet_file)
    df = df[df["instrument"] == "OPTION"].copy()
    df.sort_values("datetime", inplace=True)

    trades = []

    global_mtm = 0
    daily_target_hit = False

    results = {}

    for option_type in ["CE", "PE"]:
        opt_df = df[df["option_type"] == option_type].reset_index(drop=True)

        marked_price = opt_df.loc[0, "marked_price"]
        buffer = marked_price + BUFFER_POINTS

        state = "IDLE"
        entry_price = None
        entry_time = None
        tsl = None
        sl = None
        max_price = None
        option_target_hit = False
        wait_for_reentry_reset = False

        for i in range(1, len(opt_df) - 1):
            row = opt_df.iloc[i]
            next_row = opt_df.iloc[i + 1]

            avg_price = calc_avg_price(row)

            # =======================
            # ENTRY
            # =======================
            if state == "IDLE" and not daily_target_hit:
                if global_mtm <= MAX_DAILY_LOSS or not wait_for_reentry_reset:
                    if (
                        row.close > buffer
                        and avg_price > buffer
                        and avg_price < row.close
                    ):
                        entry_price = next_row.open
                        entry_time = next_row.datetime
                        state = "IN_TRADE"
                        max_price = entry_price
                        wait_for_reentry_reset = False
                        continue

            # =======================
            # IN TRADE
            # =======================
            if state in ["IN_TRADE", "TSL_ACTIVE"]:
                max_price = max(max_price, row.high)
                pnl = (row.close - entry_price) * LOT_SIZE

                # TARGET HIT
                if pnl >= TARGET_POINTS:
                    trades.append({
                        "option_type": option_type,
                        "entry_time": entry_time,
                        "entry_price": entry_price,
                        "exit_time": row.datetime,
                        "exit_price": row.close,
                        "pnl": pnl,
                        "exit_reason": "TARGET"
                    })
                    global_mtm += pnl
                    daily_target_hit = True
                    state = "EXITED"
                    break

                # MARKED PRICE EXIT
                if row.close < marked_price:
                    trades.append({
                        "option_type": option_type,
                        "entry_time": entry_time,
                        "entry_price": entry_price,
                        "exit_time": row.datetime,
                        "exit_price": row.close,
                        "pnl": pnl,
                        "exit_reason": "MARKED_PRICE"
                    })
                    global_mtm += pnl
                    state = "IDLE"
                    wait_for_reentry_reset = True
                    continue

                # TSL ACTIVATE
                if state == "IN_TRADE" and max_price >= buffer + TSL_ACTIVATION:
                    tsl = buffer + TSL_ACTIVATION
                    sl = tsl - SL_GAP
                    state = "TSL_ACTIVE"

                # TSL MOVE
                if state == "TSL_ACTIVE":
                    while max_price >= tsl + TSL_STEP:
                        tsl += TSL_STEP
                        sl += TSL_STEP

                    if row.close < sl:
                        trades.append({
                            "option_type": option_type,
                            "entry_time": entry_time,
                            "entry_price": entry_price,
                            "exit_time": row.datetime,
                            "exit_price": row.close,
                            "pnl": pnl,
                            "exit_reason": "TSL"
                        })
                        global_mtm += pnl
                        state = "IDLE"
                        wait_for_reentry_reset = True
                        continue

        results[option_type] = trades

    trades_df = pd.DataFrame(trades)
    print("\n✅ BACKTEST COMPLETE\n")
    print(trades_df)
    print("\n📊 Net MTM:", trades_df["pnl"].sum() if not trades_df.empty else 0)

    return trades_df


# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    run_backtest("crudeoil_intraday_2026-01-05.parquet")
