import pandas as pd

letter = ['A', 'B', 'C', 'D', 'E', 'F']
grades = [1, 2, 3, 4, 5, 6]
scores = [50, 40, 30, 20, 10, 0]

lookup = pd.read_csv('./data/raw/School Lookup.csv')
east = pd.read_csv('./data/raw/East Students.csv', parse_dates=['Date of Birth'])
west = pd.read_csv('./data/raw/West Students.csv', parse_dates=['Date of Birth'])


# fix student_id
east['Student ID'] = east['Student ID'].str[4:]
west['Student ID'] = west['Student ID'].str[:-5]

# add regions and combine datasets
east['Region'] = 'EAST'
west['Region'] = 'WEST'
df = east.append(west)

# combine first + last into full name
df['Full Name'] = df['First Name'].str.upper() + " " + df['Last Name'].str.upper()

# map value grade to letter grade and assign grade score
grades_map = {x[0]: x[1] for x in zip(grades, letter)}
df['Grade'] = df['Grade'].map(grades_map).fillna(df['Grade'])

scores_map = {x[0]: x[1] for x in zip(letter, scores)}
df['Grade Score'] = df['Grade'].map(scores_map)

# filter for wanted columns
keep_cols = ['Student ID', 'Full Name', 'Date of Birth', 'Region', 
             'Subject', 'Grade', 'Grade Score']

df = df[keep_cols]

# get a table of scores and a final table
score_table = df.pivot(index='Student ID', columns='Subject', values='Grade Score')
score_table['sum'] = score_table.sum(axis=1)

lookup['Student ID'] = lookup['Student ID'].astype(str)
lookup = lookup.set_index('Student ID')

df = (df.pivot(
        index=['Student ID', 'Full Name', 'Date of Birth', 'Region'],
        columns=['Subject'],
        values='Grade'
    ).merge(score_table['sum'], left_index=True, right_index=True)
     .merge(lookup, left_index=True, right_index=True)
     .rename(columns={'sum': 'Grade Score'})
)

df.to_csv('./data/clean/2023-W25-output.csv')