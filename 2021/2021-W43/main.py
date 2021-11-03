import pandas as pd

file = pd.ExcelFile('./data/raw/2021W43 Input.xlsx')
risk = file.parse('Risk Level')

if __name__ == '__main__':

    # load business unit A, create date lodged and merge rating value
    a = file.parse('Business Unit A ', parse_dates={'Date lodged': ['Month ', 'Date', 'Year']})
    a['Rating'] = a.merge(risk, how='left', left_on='Rating', right_on='Risk level')['Risk rating']

    # load business unit B, rename unit to business unit
    b = file.parse('Business Unit B ', skiprows=5, parse_dates=['Date lodged'], dayfirst=True)
    b = b.rename(columns={'Unit': 'Business Unit '})

    # combine business unit A and business unit B
    df = b.append(a).sort_values(by='Ticket ID')

    # get class type of opening/new tickets
    case = df.copy()
    case.loc[case['Date lodged'] < '10/01/2021', 'Status'] = 'Opening cases'
    case.loc[case['Date lodged'] >= '10/01/2021', 'Status'] = 'New cases'

    # combine case & shape data for final output
    output = (df
        .append(case)
        .groupby(['Status', 'Rating'], as_index=False)['Ticket ID'].count()
        .pivot(index='Status', columns='Rating')
        .fillna(0)
        .xs('Ticket ID', axis=1, drop_level=True)
        .reset_index()
        .melt(id_vars='Status')
        .replace('In Progress', 'Continuing')
        .rename(columns={'value': 'Cases'})
        .sort_values(by='Rating', ascending=False)
    )

    output.to_csv('./data/clean/2021-W43-output.csv', index=False)