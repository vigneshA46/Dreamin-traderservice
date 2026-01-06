import pandas as pd

# --- CONFIG (replace with your real values) ---
LOT_SIZE = 65
LOTS = 1
FORCE_EXIT_TIME = "15:20"
MTM_SL = -5000
MTM_TARGET = 10000

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
    atm = round(fut_price / strike_interval) * strike_interval
    ce_strike = atm - 2 * strike_interval
    pe_strike = atm + 2 * strike_interval
    return ce_strike, pe_strike, atm

def run_backtest(options_df, fut_price):
    trades = []

    ce_strike, pe_strike, atm = select_strikes(fut_price)
    print(f"ATM: {atm}, 2 ITM CE: {ce_strike}, 2 ITM PE: {pe_strike}")

    ce_df = options_df[(options_df['strike'] == ce_strike) & (options_df['type'] == 'CE')].reset_index()
    pe_df = options_df[(options_df['strike'] == pe_strike) & (options_df['type'] == 'PE')].reset_index()
    n = min(len(ce_df), len(pe_df))

    # --- STATE ---
    ce_positions = []
    pe_positions = []
    day_mtm = 0
    mtm_locked = False

    for i in range(1, n):
        now = ce_df.loc[i, 'datetime']
        current_time_str = now.time().strftime("%H:%M")

        # --- FORCE EXIT ---
        if current_time_str >= FORCE_EXIT_TIME:
            for pos_list, tag, df in [(ce_positions, "CE", ce_df), (pe_positions, "PE", pe_df)]:
                for pos in pos_list:
                    if pos["open"]:
                        pnl = calculate_pnl(tag, pos["entry_price"], df.loc[i, "close"])
                        trades.append(log_trade(pos["entry_time"], now, tag,
                                                pos["entry_price"], df.loc[i, "close"], pnl, "TIME_EXIT"))
                        day_mtm += pnl
                        pos["open"] = False
            break

        if day_mtm <= MTM_SL or day_mtm >= MTM_TARGET:
            mtm_locked = True

        # --- SIGNALS ---
        ce_prev_close = ce_df.loc[i-1, 'close']
        ce_prev_vwap = ce_df.loc[i-1, 'vwap']
        pe_prev_close = pe_df.loc[i-1, 'close']
        pe_prev_vwap = pe_df.loc[i-1, 'vwap']

        # CE ENTRY
        if not mtm_locked and ce_prev_close > ce_prev_vwap:
            ce_positions.append({
                "open": True,
                "entry_price": ce_df.loc[i, 'open'],
                "entry_time": now
            })
            print(f"{now} ENTRY CE @ {ce_df.loc[i, 'open']}")

            # Check for flip: PE open? Close it
            for pos in pe_positions:
                if pos["open"]:
                    pnl = calculate_pnl("PE", pos["entry_price"], pe_df.loc[i, 'open'])
                    trades.append(log_trade(pos["entry_time"], now, "PE",
                                            pos["entry_price"], pe_df.loc[i, 'open'], pnl, "FLIP_EXIT"))
                    day_mtm += pnl
                    pos["open"] = False

        # PE ENTRY
        if not mtm_locked and pe_prev_close < pe_prev_vwap:
            pe_positions.append({
                "open": True,
                "entry_price": pe_df.loc[i, 'open'],
                "entry_time": now
            })
            print(f"{now} ENTRY PE @ {pe_df.loc[i, 'open']}")

            # Check for flip: CE open? Close it
            for pos in ce_positions:
                if pos["open"]:
                    pnl = calculate_pnl("CE", pos["entry_price"], ce_df.loc[i, 'open'])
                    trades.append(log_trade(pos["entry_time"], now, "CE",
                                            pos["entry_price"], ce_df.loc[i, 'open'], pnl, "FLIP_EXIT"))
                    day_mtm += pnl
                    pos["open"] = False

        # --- EXIT CHECKS ---
        for pos in ce_positions:
            if pos["open"]:
                close = ce_df.loc[i, 'close']
                pnl = calculate_pnl("CE", pos["entry_price"], close)
                if close < ce_df.loc[i, 'vwap'] or pnl <= MTM_SL or pnl >= MTM_TARGET:
                    trades.append(log_trade(pos["entry_time"], now, "CE",
                                            pos["entry_price"], close, pnl, "VWAP_EXIT"))
                    day_mtm += pnl
                    pos["open"] = False

        for pos in pe_positions:
            if pos["open"]:
                close = pe_df.loc[i, 'close']
                pnl = calculate_pnl("PE", pos["entry_price"], close)
                if close > pe_df.loc[i, 'vwap'] or pnl <= MTM_SL or pnl >= MTM_TARGET:
                    trades.append(log_trade(pos["entry_time"], now, "PE",
                                            pos["entry_price"], close, pnl, "VWAP_EXIT"))
                    day_mtm += pnl
                    pos["open"] = False

    print(f"\nDAY MTM: {day_mtm}")
    return pd.DataFrame(trades)
