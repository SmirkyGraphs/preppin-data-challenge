import polars as pl 

col_names = {
    "field_0": "date",
    "field_1": "flight_number",
    "field_2": "temp",
    "field_3": "class",
    "field_4": "price"
}

dest_names = {
    "field_0": "from",
    "field_1": "to"
}

# split flight details & from-to destinations
df = (pl
    .scan_csv('./data/raw/PD 2024 Wk 1 Input.csv')
    .with_columns([
        pl.col("Flight Details").str.split_exact("//", 4)
    ])
    .unnest("Flight Details")
    .rename(col_names)
    .with_columns([
        pl.col('temp').str.split_exact("-", 1)
    ])
    .unnest("temp")
    .rename(dest_names)
    .select([
        'date', 'flight_number', 'from', 'to', 'class', 'price', 
        'Flow Card?', 'Bags Checked', 'Meal Type'
    ])
).collect()

yes_flow = (df
    .filter(pl.col('Flow Card?') == 1)
    .with_columns([
        pl.lit('Yes').alias('Flow Card?')
    ])
)

no_flow = (df
    .filter(pl.col('Flow Card?') == 0)
    .with_columns([
        pl.lit('No').alias('Flow Card?')
    ])
)

yes_flow.write_csv('./data/clean/2024-W01-output-1.csv')
no_flow.write_csv('./data/clean/2024-W01-output-2.csv')