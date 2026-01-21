import pandas as pd
from datetime import datetime, timedelta
import pandas_market_calendars as mcal


from auto_fetch_data import (
    load_fno_master,
    get_nearest_nifty_fut,
    fetch_intraday,
    calculate_strikes,
    find_option,
    FROM_TIME,
    TO_TIME,
    TARGET_SYMBOL
)


def get_nse_trading_days(start, end):
    nse = mcal.get_calendar("NSE")
    schedule = nse.schedule(start_date=start, end_date=end)
    return schedule.index.strftime("%Y-%m-%d").tolist()


if __name__ == "__main__":

    START_DATE = "2026-01-01"
    END_DATE   = "2026-01-16"

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt   = datetime.strptime(END_DATE, "%Y-%m-%d")

    print("📥 Loading FNO master once...")
    fno_df = load_fno_master()

    all_days = []

    trading_days = get_nse_trading_days(START_DATE, END_DATE)

    print(f"📅 Trading Days Found: {len(trading_days)}")

    for trade_date_str in trading_days:
        trade_date = pd.to_datetime(trade_date_str)


        print(f"\n🚀 Fetching {trade_date_str}")

        try:
            from_dt = f"{trade_date_str} {FROM_TIME}"
            to_dt   = f"{trade_date_str} {TO_TIME}"

            # ---- FUT ----
            fut = get_nearest_nifty_fut(fno_df, trade_date)
            fut_df = fetch_intraday(
                fut["SECURITY_ID"],
                "NSE_FNO",
                "FUTIDX",
                from_dt,
                to_dt
            )
            fut_df["symbol"] = "NIFTY_FUT"
            fut_df["instrument"] = "FUTIDX"
            fut_df["expiry"] = fut["SM_EXPIRY_DATE"]
            fut_df["strike"] = None
            fut_df["option_type"] = None

            # ---- ATM ----
            ref_price = fut_df.iloc[0]["close"]
            atm, ce_strike, pe_strike = calculate_strikes(ref_price)

            print(f"📌 {trade_date_str} ATM {atm} CE {ce_strike} PE {pe_strike}")

            # ---- CE ----
            ce = find_option(fno_df, ce_strike, "CE", trade_date)
            ce_df = fetch_intraday(
                ce["SECURITY_ID"],
                "NSE_FNO",
                "OPTIDX",
                from_dt,
                to_dt
            )
            ce_df["symbol"] = f"NIFTY_{ce_strike}_CE"
            ce_df["instrument"] = "OPTIDX"
            ce_df["expiry"] = ce["SM_EXPIRY_DATE"]
            ce_df["strike"] = ce_strike
            ce_df["option_type"] = "CE"

            # ---- PE ----
            pe = find_option(fno_df, pe_strike, "PE", trade_date)
            pe_df = fetch_intraday(
                pe["SECURITY_ID"],
                "NSE_FNO",
                "OPTIDX",
                from_dt,
                to_dt
            )
            pe_df["symbol"] = f"NIFTY_{pe_strike}_PE"
            pe_df["instrument"] = "OPTIDX"
            pe_df["expiry"] = pe["SM_EXPIRY_DATE"]
            pe_df["strike"] = pe_strike
            pe_df["option_type"] = "PE"

            day_df = pd.concat([fut_df, ce_df, pe_df], ignore_index=True)

            day_df["trade_date"] = trade_date_str
            day_df["datetime"] = pd.to_datetime(day_df["datetime"])
            day_df["expiry"] = pd.to_datetime(day_df["expiry"], errors="coerce")
            day_df["strike"] = pd.to_numeric(day_df["strike"], errors="coerce")

            all_days.append(day_df)

        except Exception as e:
            print(f"❌ Failed {trade_date_str} -> {e}")

    final_df = pd.concat(all_days, ignore_index=True)
    final_df.sort_values(["trade_date", "datetime", "symbol"], inplace=True)

    out_file = f"vwap_range_{START_DATE}_{END_DATE}.parquet"
    final_df.to_parquet(out_file, index=False)

    print(f"\n✅ RANGE PARQUET CREATED : {out_file}")
