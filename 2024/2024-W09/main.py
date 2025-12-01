import polars as pl

data = pl.read_excel('./data/raw/PD 2024 Week 9 Input.xlsx', sheet_id=0)

customers = (data['Customer Actions']
    .sort('Date')
    .group_by(['Customer ID', 'Flight Number']).last()
    .filter(pl.col('Action') != 'Cancelled')
    .sort('Date')
    .with_columns(pl
        .col('Customer ID')
        .cum_count()
        .over(['Flight Number', 'Class'])
        .alias("seats_booked")
    )
    .join(data['Flight Details'], 
        on=['Flight Number', 'Flight Date', 'Class'], 
        how='full', 
        coalesce=True
    )
    .with_columns([
        pl.col('seats_booked').fill_null(0),
        pl.col('Date').fill_null('2024-02-28')
    ])
    .with_columns([
        pl.col('seats_booked').truediv(pl.col('Capacity')).alias('Capacity %')
    ])
    .sort(['Flight Number', 'Class', 'Date'])
)

customers.write_csv('./data/clean/2024-W09-output.csv')







