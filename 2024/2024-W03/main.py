import polars as pl
import json

# read in excel sheets -> loop over tabs and combine into 1
frames = []
df = pl.read_excel('./data/raw/PD 2024 Wk 3 Input.xlsx', sheet_id=0)
for quart, frame in df.items():
    frames.append(frame)

# load renamer
with open('./data/clean/renames.json', 'r') as f:
    renamer = json.load(f)

# combine week 3 input & rename
target = (pl
    .concat(frames)
    .with_columns([
        pl.col('Month').cast(pl.Int8)
    ])
    .rename({'Class': 'class', 'Month': 'month'})
)

# combine sales data from week 1 (flow & non flow cards) and group + sum
sales = (pl
    .concat([
        pl.read_csv('../2024-W01/data/clean/2024-W01-output-1.csv', try_parse_dates=True),
        pl.read_csv('../2024-W01/data/clean/2024-W01-output-2.csv', try_parse_dates=True)
    ])
    .with_columns([
        pl.col('class').map_elements(lambda x: "".join([z[0] for z in x.split(' ')])),
        pl.col('date').dt.month()
    ])
    .group_by(['date', 'class']).agg([
        pl.col("price").sum()
    ])
    .with_columns([
        pl.col('class').replace(renamer)
    ])
    .rename({'date': 'month'})
)

# merge sales and target data into 1 dataframe
df = (sales
    .join(target, on=['month', 'class'], how='inner')
    .with_columns([
        pl.col('price').sub(pl.col('Target')).alias('diff_to_target')
    ])
)

# safe output file
df.write_csv('./data/clean/2024-W03-output.csv')