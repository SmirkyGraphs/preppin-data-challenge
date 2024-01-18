import polars as pl
from pathlib import Path

group = pl.read_csv('./data/raw/Year Groups.csv')
files = Path('./data/raw/').glob('*Attendance.csv')

# loop over term attendance and combine into 1 file
frames = []
for f in files:
    frames.append(pl
        .read_csv(f, new_columns=['First Name'])
    )

df = (pl
    .concat(frames)
    .with_columns([ # create first + last name
        (pl.col('First Name') + " " + pl.col("Last Name")).alias('Full Name')
    ])
    .join(group, on=['First Name', 'Last Name'])
    .drop('First Name', 'Last Name')
    .group_by('Full Name').agg([
        pl.col('Year Group ').max(),
        pl.col('Days Present').sum().truediv(
            pl.col('Days Present').sum().add(pl.col('Days Absent').sum())
        )
        .mul(100)
        .round(2)
        .alias('year_attendance_rate')
    ])
    .sort(by='year_attendance_rate', descending=True)
    .with_columns([ # get the top 5% in each year group and the rank
        pl.col('year_attendance_rate').rank(method='min', descending=True).alias('rank'),
        (pl
            .when(pl.col('year_attendance_rate') >= pl.col('year_attendance_rate').quantile(0.95, 'higher').over('Year Group '))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
        )
    ])
    .filter(pl.col('literal') == True)
    .sort(by='year_attendance_rate', descending=True)
    .drop('literal')
)

top = (df
    .select(['Full Name', 'year_attendance_rate', 'rank'])
    .filter(pl.col('rank') == 1)
)

top_5_pct = (df
    .select(['Year Group ', 'Full Name', 'year_attendance_rate'])
)

top.write_csv('./data/clean/2023-W43-output-1.csv')
top_5_pct.write_csv('./data/clean/2023-W43-output-2.csv')