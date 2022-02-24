import pandas as pd
import datetime as dt

cols = ['pupil first name', 'pupil last name', 'Date of Birth']
df = pd.read_csv('./data/raw/PD 2022 Wk 2 Input.csv', parse_dates=['Date of Birth'], usecols=cols)

if __name__ == '__main__':

    # combine first + last name
    df['pupil_name'] = df['pupil first name'] + " " + df['pupil last name']

    # create column for this years birthday, get month and weekday
    df['this_year_birthday'] = df['Date of Birth'].apply(lambda x: dt.datetime(2022, x.month, x.day))

    # get month/weekday (replace sat/sun with friday)
    df['month'] = df['Date of Birth'].dt.month_name()
    df['cake_needed_on'] = df['this_year_birthday'].dt.day_name()
    df.loc[df['cake_needed_on'].str.contains('Sat|Sun'), 'cake_needed_on'] = 'Friday'

    # merge number of cakes needed by month/weekday
    cakes = df.groupby(['month', 'cake_needed_on']).size().reset_index(name='bds_per_weekday_month')
    df = df.merge(cakes, how='left', on=['month', 'cake_needed_on'])

    # order columns, remove unwanted columns and save to csv
    cols = ['pupil_name', 'Date of Birth', 'this_year_birthday', 'month', 'cake_needed_on', 'bds_per_weekday_month']
    df[cols].to_csv('./data/clean/2022-W02-output.csv', index=False)