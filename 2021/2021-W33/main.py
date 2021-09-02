import pandas as pd
from datetime import datetime

xlsx = pd.ExcelFile('./data/raw/Allchains Weekly Orders.xlsx')

if __name__ == '__main__':
    data = []
    for sheet in xlsx.sheet_names:
        df = xlsx.parse(sheet)
        df['reporting_date'] = sheet
        data.append(df)

    # merge all files into one
    df = pd.concat(data).reset_index(drop=True)

    # convert to datetime and specify format
    df['reporting_date'] = pd.to_datetime(df['reporting_date'])

    # get fullfilled orders
    full = df.iloc[df.groupby('Orders')['reporting_date'].idxmax()].copy()
    full = full[full['reporting_date'] < '2021-01-29']

    full['order_status'] = 'Fullfilled'
    full['reporting_date'] = full['reporting_date'] + pd.offsets.Day(7)

    print(full)

    # mark unfullfilled orders
    df.loc[df.groupby(['Orders']).cumcount() > 0, 'order_status'] = 'Unfullfilled Order'
    df['order_status'] = df['order_status'].fillna('New Order')

    # add fullfilled to data format date columns and save
    df = df.append(full).sort_values(by=['reporting_date', 'Orders']).reset_index(drop=True)
    df['reporting_date'] = df['reporting_date'].dt.strftime('%m/%d/%Y')
    df['Sale Date'] = df['Sale Date'].dt.strftime('%m/%d/%Y')

    cols = ['order_status', 'Orders', 'Sale Date', 'reporting_date']
    df[cols].to_csv('./data/clean/2021-W33-output.csv', index=False)