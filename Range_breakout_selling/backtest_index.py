import pandas as pd

# =========================
# CONFIG
# =========================
PARQUET_FILE = "range_breakout_index_2026-01-16.parquet"

DAY_TARGET = 77
DAY_STOP   = -39

# =========================
# LOAD DATA
# =========================
df = pd.read_parquet(PARQUET_FILE)

df["datetime"] = pd.to_datetime(df["datetime"])
df = df.sort_values("datetime").reset_index(drop=True)

nifty_5m = df[(df["symbol"] == "NIFTY") & (df["interval"] == 5)]
nifty_1m = df[(df["symbol"] == "NIFTY") & (df["interval"] == 1)]
opt_1m   = df[(df["instrument_type"] == "OPTION") & (df["interval"] == 1)]

# =========================
# FIND SETUP CANDLE
# =========================
setup = nifty_5m[nifty_5m["datetime"].dt.time == pd.to_datetime("09:55").time()]

if setup.empty:
    raise Exception("❌ 9:55 candle not found")

setup = setup.iloc[0]

top_line    = max(setup["open"], setup["close"])
bottom_line = min(setup["open"], setup["close"])
atm_ref     = setup["close"]

atm = round(atm_ref / 50) * 50
ce_strike = atm - 400
pe_strike = atm + 400

print("\n================ STRIKE SELECTION ================")
print("ATM REF :", atm_ref)
print("TOP     :", top_line)
print("BOTTOM  :", bottom_line)
print("CE STRIKE:", ce_strike)
print("PE STRIKE:", pe_strike)

# =========================
# FILTER OPTIONS
# =========================
ce_df = opt_1m[(opt_1m["option_type"] == "CE") & (opt_1m["strike"] == ce_strike)]
pe_df = opt_1m[(opt_1m["option_type"] == "PE") & (opt_1m["strike"] == pe_strike)]

if ce_df.empty or pe_df.empty:
    raise Exception("❌ Option data not found for strikes")

expiry = ce_df.iloc[0]["expiry"]

print("EXPIRY  :", expiry)

# =========================
# BACKTEST ENGINE
# =========================
trades = []

ce_pos = None
pe_pos = None

pending_ce = False
pending_pe = False

total_mtm = 0
stop_trading = False

# =========================
# LOOP
# =========================
for i in range(len(nifty_1m) - 1):

    row = nifty_1m.iloc[i]
    next_row = nifty_1m.iloc[i + 1]

    if stop_trading:
        break

    if row["datetime"].time() < pd.to_datetime("10:01").time():
        continue

    avg_price = (row["open"] + row["high"] + row["low"] + row["close"]) / 4

    # ===== SIGNALS =====

    if ce_pos is None:
        if row["close"] < bottom_line and avg_price < bottom_line and avg_price < row["close"]:
            pending_ce = True

    if pe_pos is None:
        if row["close"] > top_line and avg_price > top_line and avg_price < row["close"]:
            pending_pe = True

    # ===== EXECUTION =====

    if pending_ce:
        opt = ce_df[ce_df["datetime"] == next_row["datetime"]]
        if not opt.empty:
            price = opt.iloc[0]["open"]
            ce_pos = {
                "entry_time": opt.iloc[0]["datetime"],
                "entry_price": price,
                "best": price,
                "sl": price + 15,
                "trail": price + 30,
                "active": False,
            }
        pending_ce = False

    if pending_pe:
        opt = pe_df[pe_df["datetime"] == next_row["datetime"]]
        if not opt.empty:
            price = opt.iloc[0]["open"]
            pe_pos = {
                "entry_time": opt.iloc[0]["datetime"],
                "entry_price": price,
                "best": price,
                "sl": price + 15,
                "trail": price + 30,
                "active": False,
            }
        pending_pe = False

    # ===== CE MANAGEMENT =====

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

                    trades.append([
                        "CE", ce_strike, ce_pos["entry_time"],
                        row["datetime"], ce_pos["entry_price"],
                        price, pnl
                    ])

                    ce_pos = None

    # ===== PE MANAGEMENT =====

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

                    trades.append([
                        "PE", pe_strike, pe_pos["entry_time"],
                        row["datetime"], pe_pos["entry_price"],
                        price, pnl
                    ])

                    pe_pos = None

    # ===== DAY RISK =====

    if total_mtm >= DAY_TARGET or total_mtm <= DAY_STOP:
        stop_trading = True


# =========================
# RESULTS
# =========================
result = pd.DataFrame(trades, columns=[
    "type", "strike", "entry_time", "exit_time",
    "entry_price", "exit_price", "pnl"
])

print("\n================ TRADES ================")
print(result)

print("\n================ SUMMARY ================")
print("TOTAL TRADES:", len(result))
print("TOTAL PNL   :", round(result["pnl"].sum(), 2)*65)
print("MAX PROFIT  :", result["pnl"].max() if not result.empty else 0)
print("MAX LOSS    :", result["pnl"].min() if not result.empty else 0)
