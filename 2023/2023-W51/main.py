import polars as pl

df = pl.read_excel('./data/raw/Toy Building Tracker.xlsx', sheet_id=0, read_csv_options={"has_header": False})

# label nicest/nicer/nice list label rows to drop and add quota
toys = (df['Toy Building Tracker']
    .with_columns([(pl
        .when(pl.col('column_1').str.contains("Nicest")).then(pl.lit('Nicest'))
        .when(pl.col('column_1').str.contains("Nicer")).then(pl.lit('Nicer than Most'))
        .when(pl.col('column_1').str.contains("Nice")).then(pl.lit('Nice'))
        .otherwise(None)
        .alias('list'))
    ])
    .with_columns([
        pl.when(pl.col('list').is_not_null()).then(True).otherwise(False).alias('drop'),
        pl.when(pl.col('list').is_not_null()).then('column_2').alias('quota')
    ])
    .fill_null(strategy='forward')
)

# change column headers
renamer = toys[2:3].to_dicts().pop()
renamer['column_1'] = 'toys'
renamer['column_2'] = 'quota'
renamer['list'] = 'list'
renamer['drop'] = 'drop'
renamer['quota'] = 'num_children'

# dict to replace elf names later
names = df['Elf Name Lookup']['column_1'].str.split(' - ')[1:]
letters, names = map(list, zip(*names))

# rename columns, filter rows, pivot columns
toys = (toys
    .rename(renamer)
    .with_row_count("row_nr")
    .filter(pl.col("row_nr") > 2)
    .filter(pl.col('drop') != True)
    .drop('row_nr', 'drop', 'Fun Levels Tester', 'Chief Wrapper', 'Quality Assurance')
    .melt(id_vars = ['toys', 'quota', 'Production Manager','list', 'num_children'])
    .with_columns([ # change types & fix date + add space
        pl.col("quota").cast(pl.Float64),
        pl.col("value").cast(pl.Int64),
        pl.col("num_children").cast(pl.Int64),
        (pl.col('variable') + "-2023").str.to_date("%d-%b-%Y"),
        (
            pl.col("Production Manager").str.slice(0, length=1) + " " + 
            pl.col("Production Manager").str.slice(1, length=1)
        )
    ])
    .with_columns([ # replace production mananger names, calculate quota and running sum
        pl.col('quota') * pl.col('num_children'),
        pl.cum_sum("value").over('toys').alias('running_sum_toys_produced'),
        pl.col("Production Manager").str.replace_many(letters, names)
    ])
    .rename({"value": "toys_produced", "variable": "week"})
    .with_columns([pl # label if week is over / under quota
        .when(pl.col('running_sum_toys_produced') > pl.col('quota'))
        .then(pl.lit("Over"))
        .otherwise(pl.lit("Under"))
        .alias('over_under_quota')
    ])
    .sort('toys', 'week')
    .select([
        'list', 'num_children', 'toys', 'Production Manager', 'quota', 
        'week', 'toys_produced', 'running_sum_toys_produced', 'over_under_quota'
    ])
)

# get the aggregated table of toys
aggregated = (toys
    .group_by('list', 'toys')
    .agg([ # get max quota + total produced for each toy
        pl.col('quota').max(),
        pl.col('toys_produced').sum()
    ])
    .with_columns([ # get the total over / under value
        pl.col('toys_produced').sub(pl.col('quota')).alias('over_under_quota'),
    ])
    .with_columns([pl # if the toy is the most over quota per list -> get the value as spare_toys
        .when(pl.col('over_under_quota') == (pl.col('over_under_quota').max().over('list')))
        .then(pl.col('over_under_quota').sum().over('list'))
        .otherwise(0)
        .alias('spare_toys')
    ])
    .with_columns([pl # calculate toys ready to be gifted to not overflow sleigh
        .when(pl.col('toys_produced') >= pl.col('quota'))
        .then(pl.col('toys_produced').sub(pl.col('spare_toys')))
        .otherwise('toys_produced')
        .alias('toys_ready_to_gift')
    ])
    .select([
        'list', 'toys', 'quota', 'toys_produced', 'toys_ready_to_gift', 'spare_toys', 'over_under_quota'
    ])
)

# write output to csv
toys.write_csv('./data/clean/2023-W51-output-1.csv')
aggregated.write_csv('./data/clean/2023-W51-output-2.csv')