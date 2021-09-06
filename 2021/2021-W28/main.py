import pandas as pd
import datetime

def make_dt(date):
    day = date[0].day
    month = date[0].month
    year = int(date[1].replace(',', ''))
    
    return datetime.date(year, month, day)

if __name__ == '__main__':
    file = pd.ExcelFile('./data/raw/InternationalPenalties.xlsx')
    frames = []
    for sheet in file.sheet_names:
        df = file.parse(sheet)
        df.columns = [x.lower().strip() for x in list(df)]
        df['event'] = sheet
        frames.append(df)
        
    df = pd.concat(frames)
    
    # clean names by removing unicode and noramlize germany
    df[['winner', 'loser']] = df[['winner', 'loser']].apply(lambda x: x.str.replace('\xa0', ''), axis=0)
    df = df.replace('West Germany', 'Germany')

    # make datetime
    df['date'] = df[['date', 'event year']].apply(make_dt, axis=1)

    # melt columns (pivot wide -> tall)
    id_cols = [x for x in list(df) if x not in ['winner', 'loser']]
    df = pd.melt(df, id_vars=id_cols, var_name='outcome', value_name='team')

    # get a unique id for event + game
    df['uid'] = df['no.'].astype(str) + '-' + df['event']

    # flag if a goal was scored
    df.loc[df['outcome'] == 'winner', 'scored'] = df['winning team taker'].str.contains('scored')
    df.loc[df['outcome'] == 'loser', 'scored'] = df['losing team taker'].str.contains('scored')


    # team overall win % (at least 1 win)
    overall = df.groupby(['team', 'outcome'])['uid'].nunique().reset_index()
    overall = overall.pivot(index='team', columns='outcome').fillna(0).reset_index()
    overall.columns = ['team', 'loser', 'winner']

    overall['total'] = overall.sum(axis=1)
    overall['pct'] = overall['winner'] / overall['total']
    overall['rank'] = overall['pct'].rank(method='dense', ascending=True)

    overall = overall[overall['winner'] > 0].sort_values(by='rank')
    overall.to_csv('./data/clean/team_overall_win_pct.csv', index=False)


    # team overall kicks scored %
    kick_scored = df.groupby(['team', 'scored']).size().reset_index()
    kick_scored = kick_scored.pivot(index='team', columns='scored', values=0).fillna(0)

    kick_scored['pct'] = kick_scored[True] / kick_scored.sum(axis=1)
    kick_scored['rank'] = kick_scored['pct'].rank(method='dense', ascending=False)

    kick_scored = kick_scored.sort_values(by='rank').reset_index()
    kick_scored.columns = ['team', 'missed', 'scored', 'pct_scored', 'rank']
    kick_scored.to_csv('./data/clean/team_total_penalties_score_pct.csv', index=False)


    # sucess by kick number
    kick_num = df.groupby('penalty number')['scored'].agg(['sum', 'size']).reset_index()
    kick_num['pct'] = kick_num['sum'] / kick_num['size']
    kick_num['rank'] = kick_num['pct'].rank(method='dense', ascending=False)

    kick_num.columns = ['penalty_number', 'scored', 'total', 'pct_scored', 'rank']
    kick_num.sort_values(by='rank').to_csv('./data/clean/kick_number_score_pct.csv', index=False)