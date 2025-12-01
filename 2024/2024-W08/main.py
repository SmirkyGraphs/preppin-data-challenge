import polars as pl

benefits = (pl
    .read_excel('./data/raw/prep air loyalty.xlsx', sheet_id=2)
    .with_columns([
        pl.col("Cost").str.replace_all(' |£|per|flight|a year', '').cast(pl.Int32)
    ])
    .fill_null(0)
    .rename({"Benefit": "Benefits"})
)

def get_loyalty_table(bin_size):
    return (pl
        .read_excel('./data/raw/prep air loyalty.xlsx', sheet_id=1)
        .filter(pl.col('Tier Grouping') == bin_size)
        .with_columns([
            pl.col('Number of Flights').str.split('-').list.last().str.replace(r"\+", "").cast(pl.Int32),
            pl.col('Tier').str.split(' ').list.get(1).cast(pl.Int32),
            pl.col('Benefits').str.replace(', , ', ', ') # error in dataset
        ])
        .with_columns([
            pl.col("Benefits").str.split(', ').list.eval(pl.element())
        ])
        .explode("Benefits")
        .sort("Number of Flights")
    )

def get_customer_table(bin_size):
    return (pl
        .read_csv('./data/raw/prep air updated customers.csv', try_parse_dates=True)
        .filter(pl.col('Last Date Flown') >= pl.date(2023, 2, 21))
        .with_columns([
            (pl.col("Last Date Flown") - pl.col("First Flight")).dt.total_days().floordiv(365).add(1).alias("years_member"),
            pl.col('Number of Flights').floordiv(bin_size).alias('tier')
        ])
        .with_columns([
            (pl.col('Number of Flights') / pl.col('years_member')).alias("est_flights_yr")
        ])
        .sort("Number of Flights")
    )

if __name__ == '__main__':
    tier_groupings = [5, 10]

    for tier in tier_groupings:
        loyalty = (get_loyalty_table(tier)
            .join(benefits, how='left', on='Benefits')
            .filter(pl.col('Tier') > 0)
            .sort('Tier')
            .with_columns([
                pl.when(pl.col('Benefits').str.contains('each Year')).then(pl.col('Cost')).alias('yearly'),
                pl.when(~pl.col('Benefits').str.contains('each Year')).then(pl.col('Cost').cum_sum()).alias('per_flight')
            ])
            .with_columns([
                pl.col('per_flight').forward_fill()
            ])
            .group_by('Tier').agg(
                pl.col('yearly').sum().alias('yearly_cost'),
                pl.col('per_flight').last().alias('per_flight')
            )
            .rename({"Tier": 'tier'})
        )

        df = (get_customer_table(tier)
            .group_by('tier').agg(
                pl.col('Customer ID').len().alias('number_of_customers'),
                pl.col('est_flights_yr').sum().alias('avg_flights')
            )
            .join(loyalty, on='tier', how='left')
            .filter(pl.col('tier') > 0)
            .sort(by='tier')
            .with_columns([
                (pl.col('yearly_cost') * pl.col('number_of_customers')),
                (pl.col('per_flight') * pl.col('avg_flights'))
            ])
            .with_columns([
                (pl.col('yearly_cost') + pl.col('per_flight'))
            ])
            .drop(pl.col('per_flight'))
        )

        df.write_csv(f'./data/clean/2024-W08-output_tier_{tier}.csv')
