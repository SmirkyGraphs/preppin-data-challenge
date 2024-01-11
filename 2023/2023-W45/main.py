import polars as pl

df = pl.read_excel("./data/raw/PD 2023 Wk 45 Input.xlsx")

# forward fill if project is expensive / invoice
df = (df
    .with_columns([pl
        .when(pl.col('Project').str.contains('Expense|Invoice'))
        .then(None)
        .otherwise('Project')
        .fill_null(strategy='forward')
        .alias('Project')
    ])
)

# group by project, get total cost, invoice and profit
df = (df
    .group_by('Project')
    .agg([
        pl.col('Cost').sum(),
        pl.col('Invoiced Amount').sum()
    ])
    .with_columns([
        pl.col('Invoiced Amount').sub(pl.col('Cost')).alias('profit')
    ])
)

df.write_csv('./data/clean/2023-W45-output.csv')