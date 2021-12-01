import pandas as pd

def clean_data(df):
    df = df.dropna(how='all', axis=0)
    df = df.replace('Year', '', regex=True)
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    
    return df

def transpose(df):
    return (df
        .transpose()
        .rename(columns=df.iloc[:, 0])
        .assign(Branch=list(df)[0])
        .drop(index=list(df)[0])
        .reset_index()
        .rename(columns={'index': 'Recorded Year'})
    )

df = pd.read_csv('./data/raw/PD 2021 Wk 48.csv', usecols=[1, 2, 3])

if __name__ == '__main__':

    # split dataframe every 5 rows
    splits = df.groupby(df.index // 5)
    frames = [df for idx, df in splits]

    clean = []
    for df in frames:
        clean.append(df
            .pipe(clean_data)
            .pipe(transpose)
            .melt(id_vars=['Recorded Year', 'Branch'])    
        )
    
    # re-combine cleaned & transposed data
    df = pd.concat(clean).reset_index(drop=True)
    df['value'] = df['value'].astype(float)

    # convert (m) & (k) values and remove it from variable name
    df.loc[df['variable'].str.endswith('(m)'), 'value'] *= 1000000
    df.loc[df['variable'].str.endswith('(k)'), 'value'] *= 1000

    df['variable'] = df['variable'].str.split('(').apply(lambda x: x[0].strip())

    # save output to csv file with fixed names
    df.columns = ['Recorde Year', 'Branch', 'Measure Names', 'True Value']
    df.to_csv('./data/clean/2021-W48-output.csv', index=False)