import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = pd.read_parquet("nifty_2026-01-16.parquet")
print(df.columns)
print(df.head())
