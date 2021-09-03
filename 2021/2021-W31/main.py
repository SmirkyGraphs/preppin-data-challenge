import pandas as pd

df = pd.read_csv('./data/raw/PD 2021 Wk 31 Input.csv', parse_dates=['Date'], dayfirst=True)

if __name__ == '__main__':
    # remove returned
    df = df[df['Status'] != 'Return to Manufacturer']

    df = (df
        .groupby(['Store', 'Item']).sum().reset_index()
        .pivot(index='Store', columns='Item').reset_index()
    )

    # clean column names and get total
    df.columns = df.columns.droplevel()
    df = df.rename(columns={'': 'Store'}).rename_axis('id', axis=1)
    df['Total'] = df.sum(axis=1)

    # save output
    df.to_csv('./data/clean/2021-W31-output.csv', index=False)