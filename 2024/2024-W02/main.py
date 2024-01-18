import polars as pl
import json
from pathlib import Path

# combine files into 1
frames = []
for file in Path('./data/raw/').glob('*.csv'):
    frames.append(pl.read_csv(file, try_parse_dates=True))

# load renamer
with open('./data/clean/renames.json', 'r') as f:
    renamer = json.load(f)

# adding the optional aggregation 
df = (pl
    .concat(frames)
    .with_columns([
        pl.col('Date').dt.quarter()
    ])
    # group by quarter, flow and class -> get median, min and max
    .group_by(['Date', 'Flow Card?', 'Class']).agg([
        pl.col('Price').median().alias('median'),
        pl.col('Price').min().alias('min'),
        pl.col('Price').max().alias('max')
    ])
    .melt( # melt meadian, min max wide -> tall
        id_vars = ['Date', 'Flow Card?', 'Class'],
        variable_name = 'aggregate',
        value_name = 'price'
    )
    .pivot( # pivot by class tall -> wide
        index = ['Date', 'Flow Card?', 'aggregate'],
        columns = 'Class',
        values = 'price',
    )
    .rename(renamer)
    .sort('Quarter')
)

df.write_csv('./data/clean/2024-W02-output.csv')