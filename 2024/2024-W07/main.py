import polars as pl
from datetime import date

# normalize year to just check month + date
def before_valentines_day(relation_start):
    return relation_start.replace(year=2024) <= date(2024, 2, 14)

gifts = (pl.read_csv('./data/raw/2024-wk-7-gifts.csv')
    .with_columns(
        pl.col("Year").str.replace(r"(st|nd|rd|th)$", "", literal=False).cast(pl.Int32).alias('current')
    )
    .drop('Year')
)

df = (pl.read_csv('./data/raw/2024-wk-7-couples.csv')
    .with_columns([
        pl.col("Relationship Start").str.strptime(pl.Date, format="%B %d, %Y"),
        pl.lit(date(2024, 2, 14)).alias('current')
    ])
    .with_columns([
        pl.col("Relationship Start").map_elements(before_valentines_day, pl.Boolean).alias('start_before'),
        (pl.col('current') - pl.col("Relationship Start")).dt.total_days().truediv(365).add(1).cast(pl.Int32)

    ])
    .sort('current')
    .join(gifts, on='current')
)

df.write_csv('./data/clean/2024-W07-output.csv')