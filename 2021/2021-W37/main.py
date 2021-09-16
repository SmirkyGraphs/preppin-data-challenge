import pandas as pd

df = pd.read_csv('./data/raw/2021W37 Contract Details.csv', parse_dates=['Start Date'], dayfirst=True)

if __name__ == '__main__':
    # decompose contract length
    decompose = lambda x: [i + 1 for i in range(x)]
    df['Contract Length (months)'] = df['Contract Length (months)'].apply(decompose)
    df = df.explode('Contract Length (months)').reset_index(drop=True)

    # shift month Contract Length - 1
    df['end_month'] = (
        df["Start Date"].values.astype("datetime64[M]") 
      + df["Contract Length (months)"].values.astype("timedelta64[M]") - 1
    )

    # get payment date
    df['payment_date'] = pd.to_datetime(   
        df['end_month'].dt.year.astype(str) + '/'     
      + df['end_month'].dt.month.astype(str) + '/'
      + df['Start Date'].dt.day.astype(str)
    )

    # get cumulative monthly cost (alternativly cost * contract length)
    df['cumulative_cost'] = df.groupby('Name')['Monthly Cost'].cumsum()

    # filter columns and save file
    df = df[['Name', 'payment_date', 'Monthly Cost', 'cumulative_cost']]
    df.to_csv('./data/clean/2021-W37-output.csv', index=False)