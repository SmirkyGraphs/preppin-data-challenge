import pandas as pd

file = pd.ExcelFile('./data/raw/Trilogies Input.xlsx')

films = file.parse('Films')
trilogy = file.parse('Top 30 Trilogies')

if __name__ == '__main__':
    # split Number in Series column
    films[['film_order', 'total_films']] = films['Number in Series'].str.split('/').apply(pd.Series)

    # get average and max score for each trilogy
    df = films.groupby('Trilogy Grouping').agg(['mean', 'max']).reset_index()
    df.columns = ['Trilogy Grouping', 'avg_rating', 'max_rating']

    # rank by average then max to break ties (sort then use first method)
    df = df.sort_values(by=['avg_rating', 'max_rating'], ascending=False)
    df['Trilogy Ranking'] = df['avg_rating'].rank(method='first', ascending=False)

    # merge trilogy to get ranks & remove trilogy from name
    df = df.merge(trilogy, how='left', on='Trilogy Ranking')
    df['Trilogy'] = df['Trilogy'].str.replace('trilogy', '').str.strip()

    # merge avg trilogy information back into films
    films = films.merge(df, how='left', on='Trilogy Grouping')

    # order wanted columns and save the dataset
    cols = ['Trilogy Ranking', 'Trilogy', 'avg_rating', 'film_order', 'Title', 'Rating', 'total_films']
    films = films[cols].sort_values(by=['Trilogy Ranking', 'film_order'])
    films.to_csv('./data/clean/2021_W38_output.csv', index=False)