import pandas as pd

df = pd.read_csv('./data/raw/Bike Painting Process - Painting Process.csv')
df['datetime'] = pd.to_datetime(df['Date'] + " " + df['Time'])
df = df.drop(columns=['Date', 'Time'])

if __name__ == '__main__':
    # clean up the batches for bike type and status
    batch = (df[df['Data Parameter'].isin(['Bike Type', 'Batch Status'])]
        .drop(columns=['Data Type', 'datetime'])
        .pivot(index=['Batch No.'], columns='Data Parameter')
        .reset_index()
    )

    batch.columns = ['Batch No.', 'Batch Status', 'Bike Type']

    # clean the process name data
    process = df[df['Data Parameter'] == 'Name of Process Stage']
    process = (process
        .drop(columns=['Data Type', 'Data Parameter'])
        .rename(columns={'Data Value': 'Name of Process Step'})
    )


    # clean the target & actual data
    data = df[df['Data Parameter'].str.match(r'Target|Actual')].copy()
    data[['type', 'category']] = data['Data Parameter'].str.split(' ', 1).apply(pd.Series)

    data = (data
        .drop(columns=['Data Type', 'Data Parameter'])
        .pivot(index=['Batch No.', 'datetime', 'category'], columns='type', values=['Data Value'])
        .reset_index()
        .sort_values(by='datetime')
    )

    data.columns = ['Batch No.', 'datetime', 'Data Parameter', 'Actual', 'Target']

    # combine into final dataset
    df = (data
        .merge(batch)
        .append(process)
        .sort_values(by='datetime')
    )

    df['Name of Process Step'] = df['Name of Process Step'].fillna(method='ffill')
    df = df.dropna(axis=0, subset=['Data Parameter'])
    df.to_csv('./data/clean/2021-W39-output.csv', index=False)