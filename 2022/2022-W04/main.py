import pandas as pd

df = pd.read_csv('./data/raw/PD 2021 WK 4 - Preferences of Travel.csv')

typos = {
    "Bycycle": "Bicycle",
    "Scootr": "Scooter",
    "Scoter": "Scooter",
    "Walkk": "Walk",
    "Wallk": "Walk",
    "WAlk": "Walk",
    "Helicopeter": "Helicopter",
    "Waalk": "Walk",
    "Carr": "Car"
}

sustainable = [
    "Walk",
    "Bicycle",
    "Scooter",
    "Mum's Shoulders",
    "Dad's Shoulders",
    "Hopped",
    "Skipped",
    "Jumped"
]


if __name__ == '__main__':

    # pivot columns
    df = (df
        .melt(id_vars='Student ID')
        .rename(columns={"value": "method", "variable": "weekday"})
    )

    # fix typos of travel method
    df['method'] = df['method'].map(typos).fillna(df['method'])

    # get total number of students by weekday/method
    df = df.groupby(['weekday', 'method']).size().reset_index(name='num_trips')

    # get total number of daily trips 
    trips = (df
        .groupby('weekday', as_index=False)['num_trips'].sum()
        .rename(columns={"num_trips": "daily_trips"})
    )

    # merge number of trips and get percent of total method by day
    df = df.merge(trips, on='weekday', how='left')
    df['percent_trips'] = df['num_trips'] / df['daily_trips']

    # save final data as csv file
    df.to_csv('./data/clean/2022-W04-output.csv', index=False)