import pandas as pd

# =========================
# CONFIG
# =========================

TARGET_POINTS = 70
INITIAL_TSL_POINTS = 30   # TSL placed at +30 from entry
TRAIL_STEP = 10           # TSL & SL move by 10 points
SL_GAP = 10               # SL stays 10 points below TSL


# =========================
# LOAD DATA
# =========================

def print_contract_details(ce_df, pe_df):
    ce_row = ce_df.iloc[0]
    pe_row = pe_df.iloc[0]

    print("\n📌 CONTRACT DETAILS")
    print(f"Trade Date : {ce_row['trade_date']}")
    print(f"CE Strike  : {ce_row['strike']}")
    print(f"PE Strike  : {pe_row['strike']}")

    # If expiry exists in parquet
    if "expiry" in ce_df.columns:
        print(f"CE Expiry  : {ce_row['expiry']}")
        print(f"PE Expiry  : {pe_row['expiry']}")
    else:
        print("Expiry     : (weekly expiry used - not stored in parquet)")


def load_day_data(parquet_path):
    df = pd.read_parquet(parquet_path)
    df = df.sort_values("datetime").reset_index(drop=True)

    ce_df = df[(df["instrument"] == "OPTION") & (df["option_type"] == "CE")].copy()
    pe_df = df[(df["instrument"] == "OPTION") & (df["option_type"] == "PE")].copy()

    return ce_df, pe_df


# =========================
# PREPARE OPTION DATA
# =========================

def prepare_option_df(df):
    df = df.copy()

    # Average price: (O + H + L + C) / 4
    df["avg_price"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

    # Marked line = first candle close (09:16)
    marked_line = df.iloc[0]["close"]
    print("marketd line",marked_line)
    df["marked_line"] = marked_line

    return df


# =========================
# CORE BACKTEST (ONE SIDE)
# =========================

def run_option_backtest(df, option_type):
    trades = []

    in_trade = False
    entry_price = None
    entry_time = None
    lot_size = 1

    # Re-entry rule
    price_went_below_marked = False

    # TSL variables
    tsl_active = False
    tsl = None
    sl = None
    target_price = None

    for i in range(1, len(df) - 1):
        candle = df.iloc[i]
        next_candle = df.iloc[i + 1]

        # =========================
        # ENTRY LOGIC
        # =========================
        if not in_trade:

            # Track price coming below marked line
            if candle["close"] < candle["marked_line"]:
                price_went_below_marked = True

            entry_condition = (
                candle["close"] > candle["marked_line"] and
                candle["avg_price"] < candle["close"] and
                price_went_below_marked
            )

            if entry_condition:
                in_trade = True
                entry_price = next_candle["open"]
                entry_time = next_candle["datetime"]

                target_price = entry_price + TARGET_POINTS
                tsl = entry_price + INITIAL_TSL_POINTS
                sl = None
                tsl_active = False

                price_went_below_marked = False

            continue

        # =========================
        # TARGET EXIT (HIGHEST PRIORITY)
        # =========================
        if in_trade and candle["high"] >= target_price:
            exit_price = target_price
            exit_time = candle["datetime"]

            pnl = (exit_price - entry_price) * lot_size

            trades.append({
                "option_type": option_type,
                "entry_time": entry_time,
                "entry_price": entry_price,
                "exit_time": exit_time,
                "exit_price": exit_price,
                "lot_size": lot_size,
                "pnl": pnl,
                "exit_reason": "TARGET_HIT"
            })

            in_trade = False
            lot_size = 1
            continue

        # =========================
        # NORMAL EXIT (NO TSL)
        # =========================
        if in_trade and not tsl_active:
            if candle["close"] < candle["marked_line"]:
                exit_price = candle["close"]
                exit_time = candle["datetime"]

                pnl = (exit_price - entry_price) * lot_size

                trades.append({
                    "option_type": option_type,
                    "entry_time": entry_time,
                    "entry_price": entry_price,
                    "exit_time": exit_time,
                    "exit_price": exit_price,
                    "lot_size": lot_size,
                    "pnl": pnl,
                    "exit_reason": "MARKED_LINE_EXIT"
                })

                in_trade = False
                lot_size += 1
                continue

        # =========================
        # TSL ACTIVATION
        # =========================
        if in_trade and not tsl_active:
            if candle["high"] >= tsl:
                tsl_active = True
                sl = tsl - SL_GAP

        # =========================
        # TSL TRAILING & EXIT
        # =========================
        if in_trade and tsl_active:

            # Trail TSL & SL
            while candle["high"] >= tsl + TRAIL_STEP:
                tsl += TRAIL_STEP
                sl += TRAIL_STEP

            # Exit if candle closes below SL
            if candle["close"] < sl:
                exit_price = candle["close"]
                exit_time = candle["datetime"]

                pnl = (exit_price - entry_price) * lot_size

                trades.append({
                    "option_type": option_type,
                    "entry_time": entry_time,
                    "entry_price": entry_price,
                    "exit_time": exit_time,
                    "exit_price": exit_price,
                    "lot_size": lot_size,
                    "pnl": pnl,
                    "exit_reason": "TSL_EXIT"
                })

                in_trade = False
                lot_size = 1
                continue

    return pd.DataFrame(trades)


# =========================
# MAIN
# =========================

if __name__ == "__main__":

    PARQUET_FILE = "nifty_itm_option_strategy_2026-01-13.parquet"

    ce_df, pe_df = load_day_data(PARQUET_FILE)
    print_contract_details(ce_df, pe_df)

    ce_df = prepare_option_df(ce_df)
    pe_df = prepare_option_df(pe_df)

    ce_trades = run_option_backtest(ce_df, "CE")
    pe_trades = run_option_backtest(pe_df, "PE")

    all_trades = pd.concat([ce_trades, pe_trades], ignore_index=True)

    print("\n📊 TRADE LOG")
    print(all_trades)

    print("\n💰 PnL SUMMARY")
    print(all_trades.groupby("option_type")["pnl"].sum())

    print("\n📈 TOTAL PnL:", all_trades["pnl"].sum())
