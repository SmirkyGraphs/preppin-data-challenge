import pandas as pd

df = pd.read_csv('./data/raw/PD 2021 Wk 26 Input.csv', parse_dates=['Date'], dayfirst=True)

def rolling_calculation(cols, df, agg_type):
    """ calculates moving sum or avg +/- 3 days before a date

    input:
        cols = date, destination
        agg_type = any pandas.Dataframe.aggregate function
                   samples: [min, max, mean, sum, size]
    return:
        Revenue value as a pandas series with wanted aggregation
    """
    date = cols[0] # get min/max date
    max_date = date + pd.DateOffset(days=3)
    min_date = date + pd.DateOffset(days=-3)

    if len(cols) > 1: # filter if getting destination
        df = df[df['Destination'] == cols[1]].copy()
    
    df = df[(df['Date'] >= min_date) & (df['Date'] <= max_date)].copy()
    
    return df['Revenue'].agg(agg_type)

if __name__ == '__main__':


    df['rolling_week_sum'] = df[['Date', 'Destination']].apply(rolling_calculation, df=df, agg_type='sum', axis=1)
    df['rolling_week_avg'] = df[['Date', 'Destination']].apply(rolling_calculation, df=df, agg_type='mean', axis=1)

    all = df.groupby('Date', as_index=False).sum()
    all['rolling_week_sum'] = all[['Date']].apply(rolling_calculation, df=df, agg_type='sum', axis=1)
    all['rolling_week_avg'] = all[['Date']].apply(rolling_calculation, df=df, agg_type='mean', axis=1)
    all['Destination'] = 'All'

    df = pd.concat([df, all], axis=0)
    wanted_cols = ['Destination', 'Date', 'rolling_week_avg', 'rolling_week_sum']
    df[wanted_cols].to_csv('./data/clean/2021-W26-output.csv', index=False)