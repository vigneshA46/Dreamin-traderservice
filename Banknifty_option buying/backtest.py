import pandas as pd
from datetime import time


def backtest_banknifty_side(df, side="CE", target_points=100):
    trades = []

    df = df.copy()
    df = df[df["option_type"] == side]
    df = df.sort_values("datetime").reset_index(drop=True)

    if len(df) < 3:
        return pd.DataFrame(trades)

    # ------------------------------------------------
    # MARKED PRICE (FIRST CANDLE CLOSE)
    # ------------------------------------------------
    marked_price = df.iloc[0]["close"]

    position_open = False
    entry_price = None
    entry_time = None

    tsl = None
    sl = None
    target_price = None

    allow_reentry = True
    global_target_hit = False

    for i in range(1, len(df) - 1):
        row = df.iloc[i]
        next_row = df.iloc[i + 1]

        now_time = row["datetime"].time()

        if now_time >= time(15, 20):
            if position_open:
                trades.append({
                    "trade_date": row["trade_date"],
                    "option_type": side,
                    "strike": row["strike"],
                    "entry_time": entry_time,
                    "entry_price": entry_price,
                    "exit_time": row["datetime"],
                    "exit_price": row["close"],
                    "pnl": round(row["close"] - entry_price, 2),
                    "exit_reason": "TIME_EXIT"
                })
            break

        avg_price = (row["open"] + row["high"] + row["low"] + row["close"]) / 4

        # ---------------------------------------------
        # TARGET EXIT
        # ---------------------------------------------
        if position_open and row["high"] >= target_price:
            exit_price = target_price

            trades.append({
                "trade_date": row["trade_date"],
                "option_type": side,
                "strike": row["strike"],
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": row["datetime"],
                "exit_price": exit_price,
                "pnl": round(exit_price - entry_price, 2),
                "exit_reason": "TARGET_HIT"
            })

            global_target_hit = True
            break

        # ---------------------------------------------
        # TRAIL TSL
        # ---------------------------------------------
        if position_open:
            while row["high"] >= tsl + 10:
                tsl += 10
                sl += 10

        # ---------------------------------------------
        # STOPLOSS EXIT
        # ---------------------------------------------
        if position_open and row["low"] <= sl:
            exit_price = sl

            trades.append({
                "trade_date": row["trade_date"],
                "option_type": side,
                "strike": row["strike"],
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": row["datetime"],
                "exit_price": exit_price,
                "pnl": round(exit_price - entry_price, 2),
                "exit_reason": "TSL_HIT"
            })

            position_open = False
            allow_reentry = False
            continue

        # ---------------------------------------------
        # MARKED PRICE BREAK (RESET REENTRY)
        # ---------------------------------------------
        if row["close"] < marked_price:
            allow_reentry = True

        # ---------------------------------------------
        # ENTRY
        # ---------------------------------------------
        if (
            not position_open
            and allow_reentry
            and row["close"] > marked_price
            and avg_price < row["close"]
        ):
            entry_price = next_row["open"]
            entry_time = next_row["datetime"]

            target_price = entry_price + target_points
            tsl = entry_price + 30
            sl = tsl - 20

            position_open = True
            allow_reentry = False

    return pd.DataFrame(trades)


def run_banknifty_backtest(parquet_file):
    df = pd.read_parquet(parquet_file)

    ce = backtest_banknifty_side(df, "CE")
    pe = backtest_banknifty_side(df, "PE")

    all_trades = pd.concat([ce, pe], ignore_index=True)

    if not all_trades.empty:
        all_trades["net_pnl"] = all_trades["pnl"].cumsum()

    return all_trades

if __name__ == "__main__":
    PARQUET_FILE = "banknifty_option_buying_2026-01-07.parquet"

    print("📊 Running BankNifty backtest...")
    trades = run_banknifty_backtest(PARQUET_FILE)

    print("\n✅ BACKTEST COMPLETE\n")
    print(trades)

    if not trades.empty:
        print("\n📈 SUMMARY")
        print("Total Trades :", len(trades))
        print("Net PnL      :", (trades["pnl"].sum())*65)
