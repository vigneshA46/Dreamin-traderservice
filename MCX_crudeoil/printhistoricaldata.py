import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = pd.read_parquet("crudeoil_intraday_2026-01-14.parquet")

print(df.head(10))
