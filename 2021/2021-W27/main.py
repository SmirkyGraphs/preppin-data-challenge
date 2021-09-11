import pandas as pd
from tqdm import tqdm

file = pd.ExcelFile('./data/raw/PD 2021 Wk 27 Input.xlsx')

def run_lottery(df):
    """ runs a simulated NBA lottery draft using dataframe of team's seed
    """
    lottery = pd.DataFrame(columns={"Seed", "Team", "draft_pick", "prob"})
    for pick in df['draft_pick'].unique():
        pick_df = df[(df['draft_pick'] == pick) & (~df['Team'].isin(lottery['Team']))]

        if pick_df['final'].notnull().sum() > 0:
            winner = pick_df[pick_df['final']==True].drop(columns='final')
            lottery = lottery.append(winner, ignore_index=True)
        else:
            winner = pick_df.sample(1, weights='prob').drop(columns='final')
            lottery = lottery.append(winner, ignore_index=True)

    return lottery

if __name__ == '__main__':
    # import tables & merge together
    teams = file.parse('Teams')
    seed = file.parse('Seeding').merge(teams, how='left')
    df = seed.melt(id_vars=['Seed', 'Team'], var_name='draft_pick', value_name='prob').dropna()

    # convert types (replace >0.0 with 0.001 for weights)
    df['draft_pick'] = df['draft_pick'].astype(int)
    df['prob'] = df['prob'].replace('>0.0', '0.001').astype(float)

    # add final draft pick for teams if they failed to get lucky
    df.loc[df.groupby(['Team'])['draft_pick'].idxmax(), 'final'] = True

    # run the lottery and save results
    lottery = run_lottery(df)
    lottery.to_csv('./data/clean/2021-W27-output.csv', index=False)

    # run the lottery 1000 times and test outcome
    frames = []
    for i in tqdm(range(1000)):
        frames.append(run_lottery(df))
    test = pd.concat(frames)
    test['draft_pick'] = test['draft_pick'].astype(int)

    # get avg by team
    team_avg = test.groupby('Team', as_index=False)['draft_pick'].mean()
    team_avg = team_avg.rename(columns={'draft_pick': 'team_avg'})

    # get final test table and save results
    test = (test
        .pivot_table(index=['Seed', 'Team'], columns='draft_pick', aggfunc='size')
        .reset_index()
        .fillna(0)
        .merge(team_avg, how='left', on='Team')
    )

    test.to_csv('./data/clean/2021-W27-simulate-1000.csv', index=False)