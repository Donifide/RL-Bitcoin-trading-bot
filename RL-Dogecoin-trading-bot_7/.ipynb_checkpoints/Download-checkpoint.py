import datetime
import time
import pandas as pd

# Define query parameters
pair = 'DOGEUSDT' # Currency pair of interest
TIMEFRAME = '1h'#,'4h','1h','15m','1m'
TIMEFRAME_S = 3600 # seconds in TIMEFRAME

import pyarrow.parquet as pq
#Force true
df = pq.read_table('DOGE-USDT.parquet').to_pandas().reset_index(drop=False)
df = df.set_index('open_time')['close'].resample('1h').ohlc().reset_index(drop=False)
df.rename(columns={'open_time':'Date','open': 'Open', 'high': 'High', 'low':'Low', 'close':'Close', 'volume':'Volume'}, inplace=True)
df.to_csv(f"{pair}_{TIMEFRAME}_.csv")


