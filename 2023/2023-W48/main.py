import polars as pl
import datetime as dt

year = 2023

def get_report_dates(year):
    """ 
    parameters:
        year(int): the selected calander year

    returns:
        dataframe of full calander year

    """ 
    report_start = (pl
        .date_range(dt.date(year, 2, 1) - dt.timedelta(7), dt.date(year, 2, 1), "1d", eager=True)
        .to_frame()
        .filter(pl.col('date').dt.weekday() == 1)
    ).item()

    df = (pl
        .date_range(report_start, report_start + dt.timedelta(363), "1d", eager=True).to_frame()
        .with_columns(reporting_year = year)
    )

    return df

if __name__ == '__main__':

    combined = []
    for x in [year - 1, year]:
        combined.append(get_report_dates(x))

    df = (pl
        .concat(combined)
        .sort('date')
        .with_columns(
            (pl.col("reporting_year").cum_count() + 1).over('reporting_year').alias('reporting_day'),
        )
        .with_columns(
            (pl.when(pl.col('reporting_day') % 7 == 1).then(1).otherwise(None).alias('week_start')),
            (pl.when(pl.col('reporting_day') % 28 == 1).then(1).otherwise(None).alias('month_start'))
        )
        .with_columns(
            (pl.when(pl.col('week_start') != 0).then(pl.col('week_start').cum_sum().over(pl.col('reporting_year'))).alias('reporting_week')),
            (pl.when(pl.col('month_start') != 0).then(pl.col('month_start').cum_sum().over(pl.col('reporting_year'))).alias('reporting_month'))
        )
        .fill_null(strategy="forward")
        .filter(pl.col('date').dt.year() == year)
        .select(['date', 'reporting_year', 'reporting_month', 'reporting_week', 'reporting_day'])
    )

    df.write_csv('./data/clean/2023-W48-output.csv')