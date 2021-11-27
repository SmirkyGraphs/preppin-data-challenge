import pandas as pd

def percent_won(df):
    df['percent_won'] = df['events_won'] / df['number_of_events']
    df = df.drop(columns=['events_won'])
    
    return df

file = pd.ExcelFile('./data/raw/top_female_poker_players_and_events.xlsx')

top_100 = file.parse('top_100')
top_events = file.parse('top_100_poker_events')

if __name__ == '__main__':

    # calculate length of the players career
    career_len = top_events.groupby('player_id')['event_date'].agg(['min', 'max']).reset_index()
    career_len['career_length'] = (career_len['max'] - career_len['min']).dt.days / 365

    # calculate the number of unique countries played in
    countries = top_events.groupby('player_id', as_index=False)['event_country'].nunique()
    countries.columns = ['player_id', 'countries_visited']

    # calculate the total winnings and biggest prize won and number of events attended
    winnings = top_events.groupby('player_id')['prize_usd'].agg(['sum', 'max', 'size']).reset_index()
    winnings.columns = ['player_id', 'total_prize_money', 'biggest_win', 'number_of_events']

    # calculate how many times the player game in first place 
    first = (top_events
        .groupby('player_id').apply(lambda x: (x['player_place']== '1st').sum())
        .reset_index(name='events_won')
    )

    # combine all aggregations on player_id and get percent won
    player = (top_100[['player_id', 'name']]
        .merge(career_len[['player_id', 'career_length']], how='left', on='player_id')
        .merge(countries, how='left', on='player_id')
        .merge(winnings, how='left', on='player_id')
        .merge(first, how='left', on='player_id')
        .drop(columns=['player_id'])
        .pipe(percent_won)
    )

    # pivot the columns (wide -> tall) add rank & save
    df = (player.melt(id_vars=['name']))
    df['rank'] = df.groupby(['variable']).rank()
    df.columns = ['name', 'metric', 'raw_value', 'scaled_value']
    df.to_csv('./data/clean/2021-W47-output.csv', index=False)