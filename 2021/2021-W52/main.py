import pandas as pd

def assign_keywords(string, dictx):
    match = []
    for keyword in dictx:
        k = keyword.lower()        
        [match.append(keyword) for x in string.split(' ') if k in x.lower()]
    
    if len(match) > 0:
        return ' '.join(match)
    else:
        return 'other'
    
file = pd.ExcelFile('./data/raw/PD 2021 Wk 52 Input.xlsx')

complaints = file.parse('Complaints')
dept = file.parse('Department Responsbile')
dept = dict(zip(dept['Keyword'], dept['Department']))

if __name__ == '__main__':
    # get count of complaints per person and merge based on name
    df = (complaints
        .merge(complaints.groupby('Name', as_index=False).size())
        .rename(columns={"size": 'Complaints per Person'})
    )

    # get matching keywords within a complaint and assign a department
    df['Complaint causes'] = complaints['Complaint'].apply(lambda x: assign_keywords(x, dept))
    df['Department'] = df['Complaint causes'].replace(dept.keys(), list(map(str, dept.values())), regex=True)

    # format with commas and remove duplicates from department
    df['Complaint causes'] = df['Complaint causes'].str.split().apply(lambda x: ', '.join(x))
    df['Department'] = df['Department'].str.split().apply(lambda x: ', '.join(list(set(x))))
    df['Department'] = df['Department'].str.replace('other', 'Unknown')

    # order columns and save output to csv file
    cols = ['Complaint', 'Complaints per Person', 'Complaint causes', 'Department', 'Name']
    df[cols].to_csv('./data/clean/2021-W52-output.csv', index=False)