import polars as pl

df = pl.DataFrame({
    "Date": ["2024-01-01", "2024-12-31"]
})

df = (df
    .with_columns(
        pl.col("Date").str.to_date("%Y-%m-%d")
    )
    .upsample("Date", every="1d")
    .with_columns([
        (pl.int_range(0, pl.len()) // 28 + 1).cast(str).str.zfill(2).alias("New_Month"),
        (pl.int_range(0, pl.len()) % 28 + 1).cast(str).str.zfill(2).alias("New_Date")
    ])
    .with_columns([
        pl.col("Date").cast(str),
        (pl.lit('2024-') + pl.col('New_Month') + "-" + pl.col('New_Date')).alias('New_Date')
    ])
    .drop('New_Month')
    .filter(
        pl.col('Date').str.split('-').list.get(1) != pl.col("New_Date").str.split('-').list.get(1)
    )
)

df.write_csv('./data/clean/2024-W11-output.csv')