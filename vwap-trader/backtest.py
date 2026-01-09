import pandas as pd
from datetime import time

# --- CONFIG (replace with your real values) ---'

LOT_SIZE = 65
LOTS = 1
MTM_SL = -3000
MTM_TARGET = 10000
START_TIME = time(9, 16)     # 09:16 IST
FORCE_EXIT_TIME = time(15, 20)  # 15:20 IST
MIN_HOLD_CANDLES = 3
COOLDOWN_CANDLES = 3
VWAP_BUFFER = 0.0015   # 0.15%
MAX_HOLD_MINUTES = 15



def calculate_pnl(pos, entry_price, exit_price):
    return (exit_price - entry_price) * LOT_SIZE * LOTS if pos == "CE" else (entry_price - exit_price) * LOT_SIZE * LOTS

def log_trade(entry_time, exit_time, pos, entry, exit, pnl, reason):
    print(f"{exit_time} EXIT {pos} @ {exit} | PNL: {pnl:.2f} | {reason}")
    return {
        "entry_time": entry_time,
        "exit_time": exit_time,
        "type": pos,
        "entry": entry,
        "exit": exit,
        "pnl": pnl,
        "reason": reason
    }

def select_strikes(fut_price, strike_interval=50):
    atm =  round(fut_price / strike_interval) * strike_interval
    ce_strike = atm - 2 * strike_interval
    pe_strike = atm + 2 * strike_interval
    return ce_strike, pe_strike, atm

def run_backtest(options_df, fut_price):
    trades = []

    ce_strike, pe_strike, atm = select_strikes(fut_price)
    print(f"ATM: {atm}, 2 ITM CE: {ce_strike}, 2 ITM PE: {pe_strike}")

    ce_df = options_df[
        (options_df['strike'] == ce_strike) &
        (options_df['option_type'] == 'CE')
    ].reset_index(drop=True)

    pe_df = options_df[
        (options_df['strike'] == pe_strike) &
        (options_df['option_type'] == 'PE')
    ].reset_index(drop=True)

    n = min(len(ce_df), len(pe_df))

    # --- POSITION STATE ---
    ce_pos = None
    pe_pos = None

    day_mtm = 0
    mtm_locked = False

    for i in range(1, n):
        now = ce_df.loc[i, 'datetime']
        current_time = now.time()

        # --- BEFORE MARKET START ---
        if current_time < START_TIME:
            continue

        # =========================
        # FORCE EXIT AT 15:20 IST
        # =========================
        if current_time >= FORCE_EXIT_TIME:
            if ce_pos:
                exit_price = ce_df.loc[i, 'close']
                pnl = calculate_pnl("CE", ce_pos["entry_price"], exit_price)
                trades.append(log_trade(
                    ce_pos["entry_time"], now, "CE",
                    ce_pos["entry_price"], exit_price, pnl, "TIME_EXIT"
                ))
                day_mtm += pnl
                ce_pos = None

            if pe_pos:
                exit_price = pe_df.loc[i, 'close']
                pnl = calculate_pnl("PE", pe_pos["entry_price"], exit_price)
                trades.append(log_trade(
                    pe_pos["entry_time"], now, "PE",
                    pe_pos["entry_price"], exit_price, pnl, "TIME_EXIT"
                ))
                day_mtm += pnl
                pe_pos = None

            break  # STOP processing after 15:20


        # =========================
        # MTM LOCK
        # =========================
        if (day_mtm <= MTM_SL or day_mtm >= MTM_TARGET) and not mtm_locked:
            mtm_locked = True

            # FORCE EXIT ALL OPEN POSITIONS
            if ce_pos:
                exit_price = ce_df.loc[i, 'close']
                pnl = calculate_pnl("CE", ce_pos["entry_price"], exit_price)
                trades.append(log_trade(
                ce_pos["entry_time"], now, "CE",
                ce_pos["entry_price"], exit_price, pnl, "MTM_EXIT"
                ))
                day_mtm += pnl
                ce_pos = None

            if pe_pos:
                exit_price = pe_df.loc[i, 'close']
                pnl = calculate_pnl("PE", pe_pos["entry_price"], exit_price)
                trades.append(log_trade(
                pe_pos["entry_time"], now, "PE",
                pe_pos["entry_price"], exit_price, pnl, "MTM_EXIT"
                ))
                day_mtm += pnl
                pe_pos = None

            break  # STOP DAY COMPLETELY



        if current_time >= FORCE_EXIT_TIME:
            continue

        # =========================
        # CE DATA
        # =========================
        ce_curr_close = ce_df.loc[i, 'close']
        ce_curr_vwap  = ce_df.loc[i, 'vwap']
        ce_prev_close = ce_df.loc[i-1, 'close']
        ce_prev_vwap  = ce_df.loc[i-1, 'vwap']

        # ---- CE ENTRY ----
        if (
            not mtm_locked
            and ce_pos is None
            and ce_curr_close > ce_curr_vwap
            and ce_prev_close <= ce_prev_vwap
            and ce_curr_close > ce_curr_vwap
        ):
            ce_pos = {
            "entry_price": ce_curr_close,   # 🔥 use CLOSE
            "entry_time": now
            }
            print(f"{now} ENTRY CE @ {ce_pos['entry_price']}")

        # ---- CE EXIT ----
        if (
            ce_pos
            and ce_prev_close >= ce_prev_vwap
            and ce_curr_close < ce_curr_vwap
            ):
            exit_price = ce_curr_close
            pnl = calculate_pnl("CE", ce_pos["entry_price"], exit_price)
            trades.append(log_trade(
            ce_pos["entry_time"], now, "CE",
            ce_pos["entry_price"], exit_price, pnl, "VWAP_EXIT"
        ))
            day_mtm += pnl
            ce_pos = None


        # =========================
        # PE DATA
        # =========================
        pe_curr_close = pe_df.loc[i, 'close']
        pe_curr_vwap  = pe_df.loc[i, 'vwap']
        pe_prev_close = pe_df.loc[i-1, 'close']
        pe_prev_vwap  = pe_df.loc[i-1, 'vwap']

        # ---- PE ENTRY ----
        if (
        not mtm_locked
        and pe_pos is None
        and pe_prev_close <= pe_prev_vwap
        and pe_curr_close > pe_curr_vwap
        ):
            pe_pos = {
            "entry_price": pe_curr_close,
            "entry_time": now
            }
            print(f"{now} ENTRY PE @ {pe_pos['entry_price']}")

        # ---- PE EXIT ----
        if (
        pe_pos
        and pe_prev_close >= pe_prev_vwap
        and pe_curr_close < pe_curr_vwap
        ):
            exit_price = pe_curr_close
            pnl = calculate_pnl("PE", pe_pos["entry_price"], exit_price)
            trades.append(log_trade(
            pe_pos["entry_time"], now, "PE",
            pe_pos["entry_price"], exit_price, pnl, "VWAP_EXIT"
            ))
            day_mtm += pnl
            pe_pos = None


    print(f"\nDAY MTM: {day_mtm:.2f}")
    return pd.DataFrame(trades)
 