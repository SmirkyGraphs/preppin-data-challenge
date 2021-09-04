import pandas as pd
import datetime as dt

df = pd.read_csv('./data/raw/2021W30.csv')

if __name__ == '__main__':
    df['time'] = df[['Hour', 'Minute']].apply(lambda x: dt.datetime(2021, 7, 12, x['Hour'], x['Minute']), axis=1)

    # change "B" to -1 and "G" to 0 and calculate how far it travels
    df[['From', 'To']] = df[['From', 'To']].replace('G', '0').replace('B', '-1').astype(int)
    df['travel'] = abs(df['From'].shift(-1) - df['To'])

    # sort values by time
    df = df.sort_values(by='time')

    # get most used floor people start at and travel from it
    default = df['From'].mode().iloc[0]
    df['travel_default'] = abs(df['From'].shift(-1) - default)

    cols = {
        "default position": 'G', 
        "avg_from_default": df['travel_default'].mean(),
        "avg_from_current": df['travel'].mean(),
        "difference": df['travel'].mean() - df['travel_default'].mean()
    }

    pd.DataFrame([cols]).to_csv('./data/clean/2021-W33-output.csv', index=False)