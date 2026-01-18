import pandas as pd
from datetime import time

# ============================================================
# OPTION SIDE BACKTEST (CE / PE INDEPENDENT)
# ============================================================
def backtest_option_side(df, side="CE", daily_target=50):
    trades = []

    df = df.copy()
    df = df[df["option_type"] == side]
    df = df.sort_values("datetime").reset_index(drop=True)

    if len(df) < 3:
        return pd.DataFrame(trades)

    # --------------------------------------------------------
    # MARKED PRICE (2nd candle close → 09:15–09:16)
    # --------------------------------------------------------
    marked_price = df.iloc[1]["close"]
    position_open = False
    entry_price = None
    entry_time = None
    lot_size = 1
    cumulative_pnl = 0
    trading_disabled = False

    print("marked price for side",side,marked_price)

    # --------------------------------------------------------
    # MAIN LOOP (from 09:17 onwards)
    # --------------------------------------------------------
    for i in range(2, len(df) - 1):
        row = df.iloc[i]
        next_row = df.iloc[i + 1]

        current_time = row["datetime"].time()

        # Hard square-off time
        if current_time >= time(15, 20):
            if position_open:
                exit_price = row["close"]
                pnl = (exit_price - entry_price) * lot_size

                trades.append({
                    "trade_date": row["trade_date"],
                    "option_type": side,
                    "strike": row["strike"],
                    "entry_time": entry_time,
                    "entry_price": entry_price,
                    "exit_time": row["datetime"],
                    "exit_price": exit_price,
                    "lot_size": lot_size,
                    "pnl": round(pnl, 2),
                    "exit_reason": "TIME_EXIT"
                })

            break

        average_price = (row["open"] + row["high"] + row["low"] + row["close"]) / 4
        # ----------------------------------------------------
        # TARGET EXIT (INTRATRADE)
        # ----------------------------------------------------
        if position_open and row["high"] >= target_price:
            exit_price = target_price
            pnl = (exit_price - entry_price) * lot_size
            cumulative_pnl += pnl

            trades.append({
                "trade_date": row["trade_date"],
                "option_type": side,
                "strike": row["strike"],
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": row["datetime"],
                "exit_price": exit_price,
                "lot_size": lot_size,
                "pnl": round(pnl, 2),
                "exit_reason": "DAILY_TARGET_HIT"
                })

            position_open = False
            trading_disabled = True   # 👈 STOP FURTHER TRADES
            break                     # 👈 END LOOP FOR THIS SIDE

        # ----------------------------------------------------
        # ENTRY
        # ----------------------------------------------------
        if (
            not position_open
            and not trading_disabled
            and row["close"] > marked_price
            and average_price < row["close"]
        ):
            entry_price = next_row["open"]
            entry_time = next_row["datetime"]
            target_price = entry_price + daily_target 
            position_open = True
            continue

        # ----------------------------------------------------
        # EXIT
        # ----------------------------------------------------
        if position_open and row["close"] < marked_price:
            exit_price = row["close"]
            pnl = (exit_price - entry_price) * lot_size
            cumulative_pnl += pnl

            trades.append({
                "trade_date": row["trade_date"],
                "option_type": side,
                "strike": row["strike"],
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": row["datetime"],
                "exit_price": exit_price,
                "lot_size": lot_size,
                "pnl": round(pnl, 2),
                "exit_reason": "MARKED_PRICE_BREAK"
            })

            position_open = False
            lot_size += 1

            if cumulative_pnl >= daily_target:
                trading_disabled = True

    return pd.DataFrame(trades)


# ============================================================
# DAY RUNNER
# ============================================================
def run_day_backtest(parquet_file):
    df = pd.read_parquet(parquet_file)

    print(df.head)

    ce_trades = backtest_option_side(df, "CE")
    pe_trades = backtest_option_side(df, "PE")

    all_trades = pd.concat([ce_trades, pe_trades], ignore_index=True)

    if not all_trades.empty:
        all_trades["net_pnl"] = all_trades["pnl"].cumsum()

    return all_trades


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    PARQUET_FILE = "nifty_option_buying_2026-01-05.parquet"

    print("📊 Running strategy-accurate backtest...")
    trades = run_day_backtest(PARQUET_FILE)

    print("\n✅ BACKTEST COMPLETE\n")
    print(trades)

    if not trades.empty:
        print("\n📈 SUMMARY")
        print("Total Trades :", len(trades))
        print("Net PnL      :", trades["pnl"].sum())
 