import polars as pl

# load events and clubs information to later join
events = pl.read_csv('./data/raw/After School Events.csv', try_parse_dates=True)
clubs = pl.read_csv('./data/raw/After School Club.csv')

df = (pl # create range of dates from event start & end, filter out weekends, add wekday + joins
    .date_range(events['Date'][0], events['Date'][-1], "1d", eager=True).to_frame('Date')
    .filter(pl.col('Date').dt.weekday() < 6)
    .with_columns([
        pl.col('Date').dt.strftime('%A').alias('Day of Week')
    ])
    .join(events, on='Date', how='left')
    .join(clubs, on='Day of Week', how='left')
    .fill_null('N/A')
)

df.write_csv('./data/clean/2023-W44-output.csv')