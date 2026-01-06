import time
import datetime as dt
from config import *
from market_data.tv_feed import MarketData
from strategy.vwap_strategy import VWAPStrategy
from trader.paper_trader import PaperTrader
from utils.trade_logger import log_trade
from utils.logger import candle_log

md = MarketData()
strategy = VWAPStrategy()
trader = PaperTrader(CAPITAL)

print(f"\n🟢 VWAP Paper Trading Started for {SYMBOL}\n")

while True:
    now = dt.datetime.now().time()

    if now < START_TIME:
        time.sleep(5)
        continue

    if now >= END_TIME:
        if trader.entry_price:
            trade = trader.exit(last_close, "EOD_EXIT")
            log_trade(trade)
        print("⏹️ Market closed. Engine stopped.")
        break

    df = md.get_data(SYMBOL, EXCHANGE, 10)
    df = strategy.calculate_vwap(df)

    last = df.iloc[-1]
    last_close = last["close"]
    last_vwap = last["vwap"]

    pnl = trader.update_pnl(last_close) if trader.entry_price else None
    candle_log(last.name, last_close, last_vwap, pnl)

    if not trader.entry_price and not strategy.trade_taken:
        entry = strategy.check_entry(last_close, last_vwap)
        if entry:
            trader.enter(last_close, entry)
            strategy.trade_taken = True

    if trader.entry_price:
        if pnl <= MTM_SL:
            trade = trader.exit(last_close, "MTM_SL")
            log_trade(trade)
            break

        if pnl >= MTM_TARGET:
            trade = trader.exit(last_close, "MTM_TARGET")
            log_trade(trade)
            break

        exit_reason = strategy.check_exit(last_close, last_vwap, trader.entry_type)
        if exit_reason:
            trade = trader.exit(last_close, exit_reason)
            log_trade(trade)
            break

    time.sleep(60)
