import pandas as pd

df = pd.read_csv('./data/raw/Charity Fundraiser.csv', parse_dates=['Date'], dayfirst=True)
dates = pd.date_range(df['Date'].min(), df['Date'].max())

if __name__ == '__main__':
    
    # add in missing dates & fill
    df = (pd.DataFrame(dates)
        .rename(columns={0: 'Date'})
        .merge(df, how='left', on='Date')
        .fillna(method='ffill')
    )

    # replace date with weekday
    df['Date'] = df['Date'].dt.day_name()

    # calculate averages / per day total
    df['days_into_fundraiser'] = df.index
    df['value_raised_per_day'] = df['Total Raised to date'] / df['days_into_fundraiser']

    avg = df.groupby('Date')['value_raised_per_day'].mean().reset_index(name='avg_raised_per_weekday')
    df = df.merge(avg, how='left', on='Date')

    # save output to clean folder
    df.to_csv('./data/clean/2021-W42-output.csv', index=False)
