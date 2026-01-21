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


print("\n📊 Running VWAP Backtest...\n")

df = pd.read_parquet("nifty_2026-01-16.parquet")
df["datetime"] = pd.to_datetime(df["datetime"])
df = df.sort_values("datetime")

# ============================
# FUTURE -> ATM SELECTION
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

ce_expiry = ce_df["expiry"].iloc[0]
pe_expiry = pe_df["expiry"].iloc[0]

print(f"ATM Strike : {atm_strike}")
print(f"CE Strike  : {ce_strike}")
print(f"PE Strike  : {pe_strike}")
print(f"CE Expiry  : {ce_expiry}")
print(f"PE Expiry  : {pe_expiry}\n")

# ============================
# VWAP
# ============================
ce_df = calculate_vwap(ce_df)
pe_df = calculate_vwap(pe_df)

# ============================
# BACKTEST ENGINE
# ============================
trades = []
positions = {"CE": None, "PE": None}
cum_mtm = 0
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

        # ENTRY
        if pos is None and price > vwap and ts.time().strftime("%H:%M") >= "09:16":
            positions[label] = {
                "type": label,
                "entry_time": ts,
                "entry_price": price,
            }

        # EXIT
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

    # ============================
    # MTM CHECK
    # ============================
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
        break


# ============================
# RESULTS
# ============================
results = pd.DataFrame(trades)

print("\n📄 ALL TRADES\n")

if not results.empty:
    ce_trades = results[results["type"] == "CE"]
    pe_trades = results[results["type"] == "PE"]

    print("\n CE trades")
    print(ce_trades)

    print("\n PE trades")
    print(pe_trades)

    ce_count = len(ce_trades)
    pe_count = len(pe_trades)
    total_count = len(results)

    ce_pnl = ce_trades["pnl"].sum()
    pe_pnl = pe_trades["pnl"].sum()
    total_pnl = results["pnl"].sum()

    print("\n📊 SUMMARY\n")
    print(f"CE Trades Count : {ce_count}")
    print(f"PE Trades Count : {pe_count}")
    print(f"Total Trades    : {total_count}\n")

    print(f"CE PNL   : {round(ce_pnl, 2)}")
    print(f"PE PNL   : {round(pe_pnl, 2)}")
    print(f"TOTAL PNL: {round(total_pnl, 2)}")

else:
    print("\nNo trades executed.")
 