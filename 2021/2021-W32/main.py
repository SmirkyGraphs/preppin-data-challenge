import pandas as pd

def days_till_flight(date_purchase, date_flight):
    if (date_flight - date_purchase).days < 7:
        return 'Less than 7 days'
    else:
        return '7 or more days'
    
col_names = {
    "mean": {
        "7 or more days": "Avg. daily sales 7 days of more until the flight",
        "Less than 7 days": "Avg. daily sales less than 7 days until the flight"
    },
    "sales": {
        "7 or more days": "Sales 7 days of more until the flight",
        "Less than 7 days": "Sales less than 7 days until the flight"
    }
}

df = pd.read_csv('./data/raw/PD 2021 Wk 32 Input - Data.csv')

if __name__ == '__main__':
    # convert dates and calculate < or >= 7 days
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    df['Date of Flight'] = pd.to_datetime(df['Date of Flight'], format='%d/%m/%Y')
    df['days_left'] = df.apply(lambda x: days_till_flight(x['Date'], x['Date of Flight']), axis=1)

    # add flight name departure + destination
    df['Flight Name'] = df['Departure'] + " to " + df['Destination']

    # get average sales by flight
    mean = df.groupby(['Flight Name', 'Class', 'days_left'])['Ticket Sales'].mean().round().reset_index()
    mean = mean.pivot(index=['Flight Name', 'Class'], columns=['days_left'], values='Ticket Sales').reset_index()
    mean.columns.name = None
    mean = mean.rename(columns=col_names['mean'])

    # get sum of sales by flight
    sales = df.groupby(['Flight Name', 'Class', 'days_left'])['Ticket Sales'].sum().round().reset_index()
    sales = sales.pivot(index=['Flight Name', 'Class'], columns=['days_left'], values='Ticket Sales').reset_index()
    sales.columns.name = None
    sales = sales.rename(columns=col_names['sales'])

    # merge and save file
    df = mean.merge(sales, on=['Flight Name', 'Class'])
    df.to_csv('./data/clean/2021-W32-output.csv', index=False)