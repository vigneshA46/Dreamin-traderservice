import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = pd.read_parquet("nifty_2026_1_1.parquet")
ce = df[df["symbol"] == "NIFTY_26050_CE"]
ce = ce.sort_values("datetime")
print(ce.iloc[0])
