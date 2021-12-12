import pandas as pd

df = pd.read_csv('./data/raw/PD 2021 Wk 49 Input.csv', dayfirst=True, parse_dates=['Date'])

if __name__ == '__main__':

    # calculate employment range and format
    emp_range = df.groupby('Name')['Date'].agg(['min', 'max']).reset_index()
    emp_range[['min', 'max']] = emp_range[['min', 'max']].apply(lambda x: x.dt.strftime('%b %Y'))
    emp_range['employment_range'] = emp_range['min'] + " to " + emp_range['max']

    # calcualte number of months worked and sales for year + annual salary
    agg_cols = {'Annual Salary': 'max', 'Sales':['sum','size']}
    df = df.groupby(['Name', df['Date'].dt.year])[['Annual Salary', 'Sales']].agg(agg_cols).reset_index()
    df.columns = ['Name', 'reporting_year', 'annual_salary', 'total_sales', 'tenure_by_end']

    # calculate salary paid, bonus and total paid
    df['bonus'] = round(df['total_sales'] * 0.05, 2)
    df['salary_paid'] = round((df['annual_salary'] / 12) * df['tenure_by_end'], 2)
    df['total_paid'] = df['salary_paid'] + df['bonus']

    # merge employment range, order and filter cols & save output
    col_order = ['Name', 'employment_range', 'reporting_year', 'tenure_by_end', 'salary_paid', 'bonus', 'total_paid']
    df = df.merge(emp_range[['Name', 'employment_range']], how='left', on='Name')
    df[col_order].to_csv('./data/clean/2021-W49-output.csv', index=False)
