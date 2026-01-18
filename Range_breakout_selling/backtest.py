import pandas as pd
from math import floor
from datetime import time

LOT_SIZE = 65

def backtest_range_breakout(parquet_path):
    df = pd.read_parquet(parquet_path)

    nifty_5m = df[(df.symbol == "NIFTY") & (df.interval == 5)].sort_values("datetime")

    ce_5m = df[(df.option_type == "CE") & (df.interval == 5)].sort_values("datetime")
    ce_1m = df[(df.option_type == "CE") & (df.interval == 1)].sort_values("datetime")

    pe_5m = df[(df.option_type == "PE") & (df.interval == 5)].sort_values("datetime")
    pe_1m = df[(df.option_type == "PE") & (df.interval == 1)].sort_values("datetime")
    
    ce_strike = ce_1m["strike"].iloc[0]
    pe_strike = pe_1m["strike"].iloc[0]
    print("CE strike ",ce_strike)
    print("PE strike ",pe_strike)
    ce_expiry = ce_1m["expiry"].iloc[0]
    pe_expiry = pe_1m["expiry"].iloc[0]
    print("CE expiry",ce_expiry)
    print("PE expiry",pe_expiry)

    ce_ref = min(ce_5m.iloc[9].open, ce_5m.iloc[9].close)
    pe_ref = min(pe_5m.iloc[9].open, pe_5m.iloc[9].close)

    print("CE marked line",ce_ref)
    print("PE marked line",pe_ref)

    ce_pos = None
    pe_pos = None
    trades = []
    day_active = True

    timeline = sorted(set(ce_1m.datetime).intersection(pe_1m.datetime))
    timeline = [t for t in timeline if t.time() > pd.to_datetime("10:00").time()]


    ce_1m = ce_1m.set_index("datetime")
    pe_1m = pe_1m.set_index("datetime")

    for t in timeline:
        if t.time() >= time(15, 20):
            break

        total_mtm = 0

        # ---- Manage positions ----
        for side in ["CE", "PE"]:
            pos = ce_pos if side == "CE" else pe_pos
            df1m = ce_1m if side == "CE" else pe_1m
            ref = ce_ref if side == "CE" else pe_ref

            if pos is None:
                continue

            row = df1m.loc[t]
            ltp = row.close

            mtm = (pos["entry_price"] - ltp) * LOT_SIZE
            pos["max_mtm"] = max(pos["max_mtm"], mtm)

            if pos["max_mtm"] >= 2000:
                trail_base = floor(pos["max_mtm"] / 1000) * 1000
                if trail_base >= 2000:
                    pos["tsl"] = trail_base - 1000

            exit_reason = None

            if pos.get("tsl") is not None and mtm <= pos["tsl"]:
                exit_reason = "TSL"

            if row.close > ref:
                exit_reason = "REF_BREAK"

            if exit_reason:
                trades.append({
                    "side": side,
                    "entry_time": pos["entry_time"],
                    "entry_price": pos["entry_price"],
                    "exit_time": t,
                    "exit_price": ltp,
                    "pnl": mtm,
                    "reason": exit_reason
                })

                if side == "CE":
                    ce_pos = None
                else:
                    pe_pos = None
                continue

            total_mtm += mtm

        # ---- Portfolio ----
        if total_mtm <= -2500 or total_mtm >= 5000:
            for side in ["CE", "PE"]:
                pos = ce_pos if side == "CE" else pe_pos
                if pos:
                    row = (ce_1m if side == "CE" else pe_1m).loc[t]
                    ltp = row.close
                    mtm = (pos["entry_price"] - ltp) * LOT_SIZE

                    trades.append({
                        "side": side,
                        "entry_time": pos["entry_time"],
                        "entry_price": pos["entry_price"],
                        "exit_time": t,
                        "exit_price": ltp,
                        "pnl": mtm,
                        "reason": "PORTFOLIO_EXIT"
                    })

            break

        # ---- Entries ----
        for side in ["CE", "PE"]:
            pos = ce_pos if side == "CE" else pe_pos
            df1m = ce_1m if side == "CE" else pe_1m
            ref = ce_ref if side == "CE" else pe_ref

            if pos is not None:
                continue

            row = df1m.loc[t]
            avg = (row.open + row.high + row.low + row.close) / 4

            if row.close < ref and avg < ref and avg > row.close:
                new_pos = {
                    "entry_price": row.close,
                    "entry_time": t,
                    "max_mtm": 0,
                    "tsl": None
                }

                if side == "CE":
                    ce_pos = new_pos
                else:
                    pe_pos = new_pos

    return pd.DataFrame(trades)


if __name__ == "__main__":
    df_trades = backtest_range_breakout("range_breakout_2026-01-16.parquet")
    print(df_trades)
    total_pnl = df_trades["pnl"].sum() if not df_trades.empty else 0 

    print("\n💰 BACKTEST SUMMARY")
    print(f"Total PnL : {round(total_pnl, 2)}")
 