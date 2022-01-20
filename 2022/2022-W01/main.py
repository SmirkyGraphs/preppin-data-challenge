import pandas as pd

df = pd.read_csv('./data/raw/PD 2022 Wk 1 Input.csv', parse_dates=['Date of Birth'])

if __name__ == '__main__':
    
    # pupils name (last_name + first_name)
    df['pupil_name'] = df['pupil last name'] + ", " + df['pupil first name']

    # parents contact (lasst_name + parent_name_1)
    df['parental_contact'] = df['pupil last name'] + ", " + df['Parental Contact Name_1']

    # parent contact email (parent name + @ + company.com)
    df['parental_contact_email'] = df['parental_contact'].str.replace(', ','.') + "@" + df['Preferred Contact Employer'] + ".com"

    # get academic year (if born on/after sept return year else return year - 1)
    df['academic_year'] = (2015 - df['Date of Birth'].map(lambda x: x.year if x.month >= 9 else x.year - 1))

    # save final file with ordered columns
    cols = ['academic_year', 'pupil_name', 'parental_contact', 'parental_contact_email']
    df[cols].to_csv('./data/clean/2022-W01-output.csv', index=False)