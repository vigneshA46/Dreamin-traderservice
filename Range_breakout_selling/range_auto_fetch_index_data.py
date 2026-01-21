import pandas as pd
from datetime import datetime
import pandas_market_calendars as mcal

from auto_fetch_index_data import (
    fetch_index_candles,
    fetch_option_candles,
    fetch_instruments,
    compute_atm,
    pick_itm5,
    normalize_df,
    TRADE_START,
    TRADE_END
)

# =========================
# CONFIG
# =========================

START_DATE = "2026-01-01"
END_DATE   = "2026-01-16"


def get_nse_trading_days(start, end):
    nse = mcal.get_calendar("NSE")
    schedule = nse.schedule(start_date=start, end_date=end)
    return schedule.index.strftime("%Y-%m-%d").tolist()


if __name__ == "__main__":

    trading_days = get_nse_trading_days(START_DATE, END_DATE)
    print(f"📅 Trading Days: {len(trading_days)}")

    all_days = []

    print("\n📦 Loading instruments once...")
    instruments = fetch_instruments()
    print("✅ Instruments loaded\n")


    for TRADE_DATE in trading_days:

        print(f"\n🚀 FETCH {TRADE_DATE}")

        try:
            # ---- INDEX DATA ----
            nifty_5m = fetch_index_candles(5)
            nifty_1m = fetch_index_candles(1)

            atm = compute_atm(nifty_5m)

            # ---- OPTIONS ----
            ce, pe = pick_itm5(instruments, atm)

            ce_1m = fetch_option_candles(ce["SECURITY_ID"], 1)
            pe_1m = fetch_option_candles(pe["SECURITY_ID"], 1)

            nifty_5norm = normalize_df(
                nifty_5m, "NIFTY", "INDEX", "13",
                None, None, None, 5
            )
            nifty_1norm = normalize_df(
                nifty_1m, "NIFTY", "INDEX", "13",
                None, None, None, 1
            )

            ce1_norm = normalize_df(
                ce_1m, ce["DISPLAY_NAME"], "OPTION", ce["SECURITY_ID"],
                "CE", ce["STRIKE_PRICE"], ce["SM_EXPIRY_DATE"], 1
            )

            pe1_norm = normalize_df(
                pe_1m, pe["DISPLAY_NAME"], "OPTION", pe["SECURITY_ID"],
                "PE", pe["STRIKE_PRICE"], pe["SM_EXPIRY_DATE"], 1
            )

            day_df = pd.concat([nifty_5norm, nifty_1norm, ce1_norm, pe1_norm])
            day_df["trade_date"] = TRADE_DATE
            print(day)

            all_days.append(day_df)

        except Exception as e:
            print(f"❌ {TRADE_DATE} FAILED -> {e}")

    final_df = pd.concat(all_days, ignore_index=True)
    final_df.sort_values(["trade_date", "datetime"], inplace=True)

    out_file = f"range_breakout_index_{START_DATE}_{END_DATE}.parquet"
    final_df.to_parquet(out_file, index=False)

    print(f"\n✅ RANGE PARQUET CREATED : {out_file}")
