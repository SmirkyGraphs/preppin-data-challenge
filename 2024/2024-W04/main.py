import polars as pl

input = pl.read_excel('./data/raw/PD 2024 Wk 4 Input.xlsx', sheet_id=0)

# combine the 2 non-flow and flow card datasets
seat_allocation = (pl
    .concat([input['Non_flow Card'], input['Non_flow Card2']])
    .with_columns([
        pl.lit(False).alias('Flow Card?')
    ])
    .vstack(input['Flow Card']
        .with_columns([
            pl.lit(True).alias('Flow Card?')
        ])
    )
)

# find total by seat, row, class and flow card then join & filter
df = (seat_allocation
    .group_by(['Seat', 'Row', 'Class', 'Flow Card?']).count()  
    .join(input['Seat Plan'], on=['Class', 'Seat', 'Row'], how='outer_coalesce')
    .fill_null(0)
    .filter(pl.col('count') == 0)
    .drop(['Flow Card?', 'count'])
)

# export to csv
df.write_csv('./data/clean/2024-W04-output.csv')


