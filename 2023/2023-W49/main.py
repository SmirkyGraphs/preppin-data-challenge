import polars as pl

df = (pl
    .scan_csv('./data/raw/Regular Savings Accounts.csv')
    .with_columns([
        pl.when(pl.col('Has Additional Conditions') == 'Y').then(
            (pl.col('Provider') + " (Conditions Apply)")
        ).otherwise(
            pl.col('Provider')
        ),
        pl.col('Interest').str.replace("%", "").cast(pl.Float32).truediv(100),
        pl.col('Max Monthly Deposit').str.replace("£", "").cast(pl.Int32),
        pl.lit([x + 1 for x in range(12)]).alias('month')
    ])
    .drop("Has Additional Conditions")
    .explode('month')
    .with_columns([ # calculate interest with listed formula
        (pl.col('Max Monthly Deposit').mul((pl.col('Interest') / 12).add(1).cum_prod()))
        .cum_sum().round(2).over('Provider').alias('savings_each_month')
    ])
    .with_columns([ # get max savings and total interest for each provider
        pl.col('savings_each_month').max().over('Provider').alias('max_savings'),
        pl.col('savings_each_month').max().sub(pl.col('Max Monthly Deposit').mul(12)).over('Provider').alias('total_interest')
    ])
    .with_columns([ # rank by max savings and total interest
        pl.col('max_savings').rank(method='dense', descending=True).alias('rank_max_savings'),
        pl.col('total_interest').rank(method='dense', descending=True).alias('rank_total_interest')
    ])
    .select([ # select and order wanted columns
        'rank_max_savings', 'rank_total_interest', 'Provider', 'Interest', 'Max Monthly Deposit', 
        'month', 'savings_each_month', 'max_savings', 'total_interest'
    ])
    .sort(['rank_total_interest', 'month'])    
)

df.collect().write_csv('./data/clean/2023-W49-output.csv')