import pandas as pd

def parse_mins_distance(mins):
    speed = 30
    hours = mins / 60
    
    return speed * hours

df = pd.read_csv('./data/raw/Carls 2021 cycling.csv', parse_dates=['Date'], dayfirst=True)

if __name__ == '__main__':

    # convert value to km ridden for min measure
    df.loc[df['Measure'] == 'min', 'Value'] = df['Value'].apply(parse_mins_distance)

    # add outdoor / turbo trainer
    df.loc[df['Measure'] == 'km', 'Measure'] = 'Outdoors'
    df.loc[df['Measure'] == 'min', 'Measure'] = 'Turbo Trainer'

    # group, pivot and add activites per day, reindex for missing dates
    df = (df
        .groupby(['Date', 'Measure'], as_index=False).sum()
        .pivot(index='Date', columns='Measure')
        .xs('Value', axis=1, drop_level=True)
        .assign(activies_per_day=df.groupby('Date').size())
        .reindex(pd.date_range('2021-01-01', '2021-11-01'))
        .fillna(0)
        .rename_axis(None, axis='columns')
        .rename_axis('Date')
        .reset_index()
    )

    df.to_csv('./data/clean/2021-W44-output.csv', index=False)



