import sys

import pandas as pd


dfs = []

for file in sys.argv[1:]:
    df = pd.read_parquet(file)
    dfs.append(df)

df = pd.concat(dfs)

df = df.sort_values('warcinfo_id')

df.to_parquet('warcinfo-id.parquet', index=False)
