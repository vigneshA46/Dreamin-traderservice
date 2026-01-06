import pandas as pd
from indicators import calculate_vwap
from backtest import run_backtest

df = pd.read_parquet("/nifty.parquet")
df = df.between_time("09:15", "15:30")

df["vwap"] = calculate_vwap(df)

trades = run_backtest(df)

df_trades = pd.DataFrame(trades)
print("\n \n df trades print in run backtest.py file \n")
print(df_trades)
print("DAY MTM:", df_trades["pnl"].sum())

print("\nSUMMARY")
print(pd.DataFrame(trades))
