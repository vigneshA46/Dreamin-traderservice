import pandas as pd

df = pd.read_parquet("range_breakout_2026-01-16.parquet")

print("\nUnique interval values:")
print(df["interval"].value_counts(dropna=False).head(20))

print("\nInterval dtype:")
print(df["interval"].dtype)
print("columns\n")
print(df.columns)
print("\n================ NIFTY 5M ================")
print(df[(df["symbol"] == "NIFTY") & (df["interval"].astype(str) == "5")].head())

print("\n================ CE 5M ===================")
print(df[(df["option_type"] == "CE") & (df["interval"].astype(str) == "5")].head())

print("\n================ CE 1M ===================")
print(df[(df["option_type"] == "CE") & (df["interval"].astype(str) == "1")].head())

print("\n================ PE 5M ===================")
print(df[(df["option_type"] == "PE") & (df["interval"].astype(str) == "5")].head())

print("\n================ PE 1M ===================")
print(df[(df["option_type"] == "PE") & (df["interval"].astype(str) == "1")].head())
