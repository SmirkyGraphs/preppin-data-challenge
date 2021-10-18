import pandas as pd
import numpy as np

def get_missing_years(year_min, year_max):    
    year_min = int(year_min.split('-')[0])
    year_max = int(year_max.split('-')[0])
    
    year_list = [x for x in range(year_min, year_max + 1)]
    year_list = [f'{str(x)}-{str(x+1)[-2:]}' for x in year_list]
    
    return year_list

df = pd.read_csv('./data/raw/Southend Stats.csv', sep='\s+')

if __name__ == '__main__':

    # get league value & fill missing ones
    df['league_value'] = df['LEAGUE'].str.extract('(\d+)').astype(float)
    df.loc[df['LEAGUE']=='FL-CH', 'league_value'] = 0
    df.loc[df['LEAGUE']=='NAT-P', 'league_value'] = 5

    # assign outcome between current / next season
    df.loc[df['league_value'] > df['league_value'].shift(-1), 'outcome'] = 'Promoted'
    df.loc[df['league_value'] < df['league_value'].shift(-1), 'outcome'] = 'Relegated'
    df.loc[df['league_value'] == df['league_value'].shift(-1), 'outcome'] = 'Same League'

    # create rows, special circumstances & fill ww1 and ww2
    data = get_missing_years(df['SEASON'].min(), df['SEASON'].max())
    data = pd.DataFrame(data).rename(columns={0: 'SEASON'})

    df['Special Circumstances'] = 'N/A' # fill all special with n/a as default
    df.loc[df['SEASON'] == '1939-40', 'Special Circumstances'] = 'Abandoned due to WW2'
    df.loc[df['SEASON'] == df['SEASON'].max(), 'Special Circumstances'] = 'Incomplete'

    # merge list of years with dataframe and rename p.1
    df = data.merge(df, how='left').rename(columns={'P.1': 'Pts'})

    # add ww1 and ww2 labels and forward fill nulls
    df.loc[df['SEASON'] == '1915-16', 'Special Circumstances'] = 'WW1'
    df.loc[df['SEASON'] == '1940-41', 'Special Circumstances'] = 'WW2'
    df['Special Circumstances'] = df['Special Circumstances'].fillna(method='ffill')

    # fill nulls for league and fix incomplete pos
    df['LEAGUE'] = df['LEAGUE'].fillna('N/A')
    df.loc[df['SEASON'] == '1939-40', 'POS'] = np.nan

    # order columns and save output
    cols = ['SEASON', 'outcome', 'Special Circumstances', 'LEAGUE', 'P', 'W', 'D', 'L', 'F', 'A', 'Pts', 'POS'] 
    df[cols].to_csv('./data/clean/2021-W41-output.csv', index=False)