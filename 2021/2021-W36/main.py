import pandas as pd

file = pd.ExcelFile('./data/raw/Trend Input.xlsx')

if __name__ == '__main__':

    # getting top country for each search term
    country = file.parse('Country Breakdown', skiprows=2).melt(id_vars='Country')
    country['variable'] = country['variable'].str.replace(': (01/09/2016 - 01/09/2021)', '', regex=False)

    countries = (country.dropna().groupby('Country').size() > 2).reset_index()
    all_3_searches = countries[countries[0] == True]['Country'].tolist()

    country = country[country['Country'].isin(all_3_searches)].copy()
    country['rank'] = country.groupby('variable')['value'].rank('dense', ascending=False)
    country = country[country['rank'] == 1].reset_index(drop=True)

    country.columns = ['highest_pct_country', 'search_term', 'country_value', 'rank']
    country = country[['search_term', 'highest_pct_country']]


    # clean up timeline and pivot rows (wide -> tall)
    tl = file.parse('Timeline', skiprows=2)
    tl = tl.melt(id_vars='Week')

    # clean variable name and add 'fiscal year' from sept -> aug
    tl['variable'] = tl['variable'].str.replace(': (Worldwide)', '', regex=False)
    tl['Year'] = pd.to_datetime(tl['Week']).dt.to_period('A-AUG')
    
    # get the first peak dates
    peak = tl[tl.index.isin(tl.groupby('variable')['value'].idxmax().values)]
    peak = peak.rename(columns={'Week': 'first_peak', 'variable': 'search_term', 'value': 'peak_index'})
    peak = peak[['search_term', 'peak_index', 'first_peak']]
                       
                    
    # get trend direction by search_term
    df = tl.groupby(['Year', 'variable']).agg(['sum', 'size']).reset_index()
    df.columns = ['year', 'search_term', 'sum', 'count']
    df['avg'] = round(df['sum'] / df['count'], 1)

    # see if trend continued by search_term from prior year
    df.loc[df['avg'] - df.groupby('search_term')['avg'].shift() > 0, 'status'] = 'Still Trendy'
    df['status'] = df['status'].fillna('Lockdown Fad')

    # get dataframe for only 2021
    df = df[df['year']=='2021']
    
    # filter wanted columns 
    df = df[['search_term', 'status', 'avg']]
    df = df.rename(columns={'avg': 'avg_index_2021'})


    # get dataframe for overall avg
    df_avg = tl.groupby('variable').agg(['sum', 'size'])
    df_avg = round(df_avg[('value', 'sum')] / df_avg[('value', 'size')], 1).reset_index()
    df_avg.columns = ['search_term', 'avg_index']

    # get final dataframe
    df = (df
        .merge(df_avg, on='search_term', how='left')
        .merge(peak, on='search_term', how='left')
        .merge(country, on='search_term', how='left')
        .sort_values(by='avg_index')
    )

    # save output
    df.to_csv('./data/clean/2021-W36-output.csv', index=False)