import polars as pl

# load data with datetime types and remove null arrival
df = (
    pl.read_csv("./data/raw/Prep School Card Entries.csv")
      .filter(pl.col("Arrival Time").is_not_null())
)

# specify types, get diff in time and amt "very late"
df = (df
    .with_columns([
        pl.col("Arrival Time").str.strptime(pl.Datetime, format="%H:%M:%S"),
        pl.col("Scheduled Start Time").str.strptime(pl.Datetime, format="%H:%M:%S"),
    ])
    .with_columns(
        (pl.col('Arrival Time') - pl.col('Scheduled Start Time')).alias('result')  
    )
    .with_columns(
        (pl.col('result').dt.minutes() > 5).alias('very_late')
    )
)

# get table of which weekdays has the highest average late time by MM:SS
weekday = (df
    .groupby("Day of Week").agg(pl.col("result").mean().cast(pl.Datetime))
    .sort("result", descending=True)
    .with_columns([
        pl.col('result').dt.minute().alias('Minutes Late'),
        pl.col('result').dt.second().alias('Seconds Late'),
        pl.col("result").rank(method="dense", descending=True).alias("rank")
    ])
    .select(["rank", "Day of Week", "Minutes Late", "Seconds Late"])
)

# get table of students ranked by % of days they were late by more then 5 mins
unique_dates = df.groupby("Student ID").agg(pl.col("Date").n_unique())
students = (df
    .groupby("Student ID").agg(pl.col("very_late").sum())
    .join(unique_dates, on='Student ID', how='left')
    .with_columns([
        (pl.col("very_late") / pl.col('Date')).alias("% Days Very Late")
    ])
    .sort("% Days Very Late", descending=True)
    .with_columns([
        pl.col("% Days Very Late").rank(method="dense", descending=True).alias("rank")
    ])
    .select(["rank", "Student ID", "% Days Very Late"])

)

weekday.write_csv("./data/clean/2023-W46-output-1.csv")
students.write_csv("./data/clean/2023-W46-output-2.csv")