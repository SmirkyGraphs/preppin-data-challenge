import pandas as pd
import numpy as np

frames = [] # list to hold parsed sections

# load data and excel sheet tabs
file = pd.ExcelFile('./data/raw/Sales Department Input.xlsx')
oct_sheet = file.parse('October', parse_dates=['Date'], dayfirst=True)
nov_sheet = file.parse('November', parse_dates=['Date'], dayfirst=True)

# split dataframe when date is null (when person is named)
oct_ppl = np.split(oct_sheet, np.where(np.isnan(oct_sheet['Date']))[0] + 1)
nov_ppl = np.split(nov_sheet, np.where(np.isnan(nov_sheet['Date']))[0] + 1)

if __name__ == '__main__':

    for df in oct_ppl[:-1]:
        df['Salesperson'] = df.iloc[-1]['Salesperson']
        df['YTD_Total'] = df.iloc[-1]['Unnamed: 7']
        df = df.drop(columns='Unnamed: 7')
        frames.append(df[:-1])

    for df in nov_ppl[:-1]:
        df['Salesperson'] = df.iloc[-1]['Salesperson']
        df['YTD_Total'] = df['Total'].sum()
        frames.append(df[:-1])

    # combine cleaned dataframes
    df = pd.concat(frames)

    # get YTD total + Nov
    ytd_total = (df
        .groupby(['Salesperson', df['Date'].dt.month])['YTD_Total'].max()
        .groupby('Salesperson').cumsum()
        .reset_index()
    )

    # add YTD total back to combined data
    df = (df
        .drop(columns='YTD_Total')
        .merge(ytd_total, how='left', 
               left_on=['Salesperson', df['Date'].dt.month], 
               right_on=['Salesperson', 'Date'])
        .drop(columns=['Date_y', 'Date', 'RowID', 'Total'])
        .rename(columns={"Date_x": "Date"})
    )

    # melt (pivot) the data (wide -> tall)
    id_vars = ['Date', 'Salesperson', 'YTD_Total']
    df.melt(id_vars=id_vars).to_csv('./data/clean/2021-W50-output.csv', index=False)