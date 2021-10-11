import pandas as pd

df = pd.read_csv('./data/raw/Austin_Animal_Center_Outcomes.csv')

if __name__ == '__main__':

    # remove dupliacte date
    df = df.drop(columns=['MonthYear'])

    # filter for cats or dogs only
    df = df[df['Animal Type'].isin(['Cat', 'Dog'])]

    # group by Adopted, Returned to Owner or Transferred or Other
    group = ['Adoption', 'Return to Owner', 'Transfer']
    df.loc[df['Outcome Type'].isin(group), 'outcome_group'] = 'Adopted, Returned to Owner or Transferred'

    df['outcome_group'] = df['outcome_group'].fillna('Other')

    # combine and get % of total by outcome and animal
    df = (df
        .groupby(['Animal Type'])['outcome_group'].value_counts(normalize=True)
        .reset_index(name='value')
        .pivot(index='Animal Type', columns='outcome_group', values='value')
    )

    # save output to csv file
    df.to_csv('./data/clean/2021-W40-output.csv', index=False)