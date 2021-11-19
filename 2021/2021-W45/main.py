import pandas as pd

def format_time(hours):
    if len(str(hours)) < 3:
        if int(hours) <= 9:
            return f"0{hours}:00:00"

        # return non 30 mins segments
        return f"{hours}:00:00"
    
    # return proper format number
    return hours

file = pd.ExcelFile('./data/raw/TC Input.xlsx')
date_sheets = file.sheet_names[:-1]

attendees = file.parse('Attendees').rename(columns={'Attendee': 'Name'})

if __name__ == '__main__':

    # combine the weekly sheets
    frames = []
    for sheet in date_sheets:
        df = file.parse(sheet)
        df['date'] = sheet.replace('th', '')
        df['date'] = df['date'] + ' 2021'
        frames.append(df)

    df = pd.concat(frames)
    
    # format date format
    df['Session Time'] = df['Session Time'].apply(format_time)
    df['date'] = df['date'].astype(str) + " " + df['Session Time'].astype(str)
    df['date'] = pd.to_datetime(df['date'], format='%d %b %Y %H:%M:%S')

    # split attendees comma and explode dataset
    df['Attendee IDs'] = df['Attendee IDs'].str.split(',')
    df = df.explode('Attendee IDs')
    df['Attendee IDs'] = df['Attendee IDs'].astype(int)

    # get a list of each attendee and all of their direct contacts
    match_cols = ['Session ID', 'Attendee IDs']
    direct = df.merge(df[match_cols], how='outer', on='Session ID')
    direct = direct.rename(columns={"Attendee IDs_x": "Attendee", "Attendee IDs_y": "Contact"})
    direct = direct[direct['Attendee'] != direct['Contact']]
    direct['contact_type'] = 'Direct Contact'

    # get a set of all direct contacts for each contact
    direct_group = (direct
        .groupby('Attendee')['Contact'].apply(set)
        .reset_index()
        .rename(columns={'Attendee': 'Contact', 'Contact': 'Indirect'})
    )

    # get indirect contacts for everyone
    df = (direct
        .merge(direct_group, how='left', on='Contact')
        .explode('Indirect')
        .drop_duplicates(subset=['Attendee', 'Indirect'])
        .drop(columns='Contact')
        .rename(columns={'Indirect': 'Contact'})
        .assign(contact_type='Indrect Contact')
    )

    indirect = df.loc[df['Attendee'] != df['Contact']]

    # combine direct and indirect contacts 
    df = direct.append(indirect)

    # remove duplicates and merge attendee names
    df = df.drop_duplicates(subset=['Subject', 'Attendee', 'Contact'])
    df['Contact'] = pd.merge(df['Contact'], attendees, how='left', left_on='Contact', right_on='Attendee ID')['Name']
    df['Attendee'] = pd.merge(df['Attendee'], attendees, how='left', left_on='Attendee', right_on='Attendee ID')['Name']

    # filter for wanted columns and save output
    df = df[['Subject', 'Attendee', 'contact_type', 'Contact']]
    df.to_csv('./data/clean/2021-W45-output.csv', index=False)