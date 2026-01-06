import requests
import pandas as pd
import io

# public NSE F&O + index instruments
url = "https://api.dhan.co/v2/instrument/NSE_FO"  # Nifty futures
url_index = "https://api.dhan.co/v2/instrument/NSE_EQ"  # Nifty index
response = requests.get(url_index)

df = pd.read_csv(io.StringIO(response.text))

nifty50 = df[
    (df['INSTRUMENT_TYPE'] == 'INDEX') &
    (df['SYMBOL_NAME'].str.contains('NIFTY', case=False)) &
    (~df['SYMBOL_NAME'].str.contains('500|BANK|MID|FIN|NEXT|GIFT', case=False))
][['SYMBOL_NAME','SECURITY_ID']]

print(nifty50)