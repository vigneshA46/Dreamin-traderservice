import pandas as pd
from datetime import time

# =============================
# CONFIG
# =============================

LOT_SIZE = 65

ENTRY_START = time(15, 30)
ENTRY_END   = time(22, 30)

BUFFER_POINTS = 7
TSL_ACTIVATION = 15
TSL_GAP = 10

TARGET_POINTS = 50
DAILY_MAX_LOSS = -2000

PARQUET_FILE = "crudeoil_intraday_2026-01-05.parquet"

# =============================
# HELPERS
# =============================

def avg_price(row):
    return (row.open + row.high + row.low + row.close) / 4


# =============================
# POSITION
# =============================

class Position:
    def __init__(self, entry_price, entry_time):
        self.entry_price = entry_price
        self.entry_time = entry_time

        self.exit_price = None
        self.exit_time = None
        self.exit_reason = None

        self.max_price = entry_price
        self.tsl_active = False
        self.tsl = None

    def mtm_points(self, price):
        return price - self.entry_price


# =============================
# BACKTEST
# =============================

def run_backtest(df):
    trades = []

    day_target_hit = False
    combined_mtm = 0

    contexts = {}

    # ---------- BUILD PER-OPTION CONTEXT ----------
    for opt in ["CE", "PE"]:
        opt_df = df[df.option_type == opt].copy()
        opt_df = opt_df.sort_values("datetime").reset_index(drop=True)

        marked_row = opt_df[opt_df.datetime.dt.time == time(15, 15)]
        if marked_row.empty:
            continue

        marked_price = marked_row.iloc[0].close
        buffer = marked_price + BUFFER_POINTS

        contexts[opt] = {
            "df": opt_df.set_index("datetime"),
            "marked": marked_price,
            "buffer": buffer,
            "position": None,
            "pending_entry": False,
            "pending_exit": False,
            "waiting_reset": False,
            "exit_reason": None
        }

    # ---------- MASTER TIME AXIS ----------
    timeline = sorted(df.datetime.unique())

    # ---------- MAIN LOOP ----------
    for t in timeline:
        for opt, ctx in contexts.items():
            opt_df = ctx["df"]

            if t not in opt_df.index:
                continue

            row = opt_df.loc[t]
            pos = ctx["position"]
            now_time = row.name.time()

            # ---------- UPDATE MTM ----------
            if pos:
                pos.max_price = max(pos.max_price, row.close)

                combined_mtm = sum(
                    c["position"].mtm_points(
                        c["df"].loc[t].close
                    ) * LOT_SIZE
                    for c in contexts.values()
                    if c["position"] and t in c["df"].index
                )

            # ---------- DAILY TARGET ----------
            if pos and pos.mtm_points(row.close) >= TARGET_POINTS:
                day_target_hit = True

            if day_target_hit:
                for c in contexts.values():
                    if c["position"] and not c["pending_exit"]:
                        c["pending_exit"] = True
                        c["exit_reason"] = "DAILY_TARGET"
                continue

            # ---------- EXIT LOGIC ----------
            if pos and not ctx["pending_exit"]:

                # Marked price exit
                if row.close < ctx["marked"]:
                    ctx["pending_exit"] = True
                    ctx["exit_reason"] = "MARKED_PRICE"

                # TSL activation
                if pos.max_price >= ctx["buffer"] + TSL_ACTIVATION:
                    if not pos.tsl_active:
                        pos.tsl_active = True
                        pos.tsl = ctx["buffer"] + TSL_ACTIVATION - TSL_GAP
                    else:
                        while pos.max_price - pos.tsl >= TSL_GAP:
                            pos.tsl += TSL_GAP

                # TSL hit
                if pos.tsl_active and row.close <= pos.tsl:
                    ctx["pending_exit"] = True
                    ctx["exit_reason"] = "TSL"

            # ---------- EXECUTE EXIT ----------
            if ctx["pending_exit"]:
                exit_price = row.open

                pos.exit_price = exit_price
                pos.exit_time = row.name
                pos.exit_reason = ctx["exit_reason"]

                trades.append({
                    "option": opt,
                    "entry_time": pos.entry_time,
                    "entry_price": pos.entry_price,
                    "exit_time": pos.exit_time,
                    "exit_price": pos.exit_price,
                    "pnl": (pos.exit_price - pos.entry_price) * LOT_SIZE,
                    "exit_reason": pos.exit_reason
                })

                ctx["position"] = None
                ctx["pending_exit"] = False
                ctx["waiting_reset"] = combined_mtm <= DAILY_MAX_LOSS
                continue

            # ---------- ENTRY LOGIC ----------
            if (
                not pos
                and not day_target_hit
                and ENTRY_START <= now_time <= ENTRY_END
            ):
                if ctx["waiting_reset"]:
                    if row.close < ctx["buffer"]:
                        ctx["waiting_reset"] = False
                    continue

                ap = avg_price(row)

                if (
                    row.close > ctx["buffer"]
                    and ap > ctx["buffer"]
                    and ap < row.close
                ):
                    ctx["pending_entry"] = True

            # ---------- EXECUTE ENTRY ----------
            if ctx["pending_entry"]:
                entry_price = row.open
                ctx["position"] = Position(entry_price, row.name)
                ctx["pending_entry"] = False

    return pd.DataFrame(trades)


# =============================
# RUN
# =============================

if __name__ == "__main__":
    df = pd.read_parquet(PARQUET_FILE)
    df["datetime"] = pd.to_datetime(df["datetime"])

    trades = run_backtest(df)

    print("\nBACKTEST COMPLETE\n")
    print(trades)
    print("\nTOTAL PNL:", trades.pnl.sum())
