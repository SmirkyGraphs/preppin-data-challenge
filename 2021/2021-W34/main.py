import pandas as pd

sheet = '1t5TbvL6ATKnUulywLvNCgj9KH38_VLv2/export?format=csv&gid'

def fetch_google_sheets(gid):
    url = f'https://docs.google.com/spreadsheets/d/{sheet}={gid}'

    return pd.read_csv(url)

def fix_names(name):
    if name.upper().startswith('ST'):
        return 'Stratford'
    elif name.upper().startswith('WI') or \
         name.upper().startswith('VI'):
        return 'Wimbledon'
    elif name.upper().startswith('BR'):
        return 'Bristol'
    elif name.upper().startswith('YO') or \
         name.upper().startswith('YA'):
        return 'York'
    
    else:
        return name

if __name__ == '__main__':
    # download files from google sheets
    emp_sales = fetch_google_sheets('295486843')
    emp_target = fetch_google_sheets('967394287')

    # fix name errors in employee target
    emp_target['Store'] = emp_target['Store'].apply(fix_names)

    # avg monthly sales
    emp_sales['avg_monthly_sales'] = round(emp_sales.mean(axis=1))

    # merge files together and filter for < 90% of target
    df = emp_sales.merge(emp_target, how='left', on=['Store', 'Employee'])
    df = df[df['avg_monthly_sales']/df['Monthly Target'] < 0.9]

    # get # of times they met/passed target value
    monthly_cols = list(df)[2:-2]
    df['target_met'] = (df[monthly_cols].sub(df['Monthly Target'], axis=0) >= 0).sum(axis=1) / len(monthly_cols)

    print(df[['Store', 'Employee', 'avg_monthly_sales', 'target_met', 'Monthly Target']])
