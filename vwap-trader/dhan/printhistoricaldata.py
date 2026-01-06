import pandas as pd

df = pd.read_parquet(
    "/nifty_2025_12_31.parquet"
)

print(df)