import pandas as pd

def clean_dates(datetime):   
    date = datetime[0].split('_')
    
    suffix = ['st', 'nd', 'rd', 'th']
    for suf in suffix:
        date[0] = date[0].replace(suf, '')
    
    datetime = f"{'_'.join(date)} {datetime[1].replace('xx', '0:00')}"

    return pd.to_datetime(datetime, format='%d_%B_%Y %H:%M')

file = pd.ExcelFile('./data/raw/Olympic Events.xlsx')

if __name__ == '__main__':
    # load sheets into dataframes
    events = file.parse(sheet_name='Olympics Events')
    venues = file.parse(sheet_name='Venues')

    # parse date format
    events['Datetime'] = events[['Date', 'Time']].apply(clean_dates, axis=1)

    # split events list and get victory & gold events
    events['Events'] = events['Events'].str.split(', ')
    events = events.explode('Events')

    events.loc[events['Events'].str.upper().str.contains('GOLD MEDAL'), 'highlight_event'] = True
    events.loc[events['Events'].str.upper().str.contains('VICTORY CEREMONY'), 'highlight_event'] = True
    events['highlight_event'] = events['highlight_event'].fillna(False)

    # fix errors in sport names
    map_names = {
        "Beach Volley": "Beach Volleyball",
        "Beach Volleybal": "Beach Volleyball",
        "Baseball": "Baseball/Softball",
        "Softball": "Baseball/Softball",
    }

    events['Sport'] = events['Sport'].str.title().str.rstrip('.')
    events['Sport'] = events['Sport'].replace(map_names)

    # merge venues location and wanted columns save
    df = events.merge(venues, how='left', on=['Venue', 'Sport'])

    cols = ['Datetime', 'Sport', 'Events', 'highlight_event', 'Venue', 'Location']
    df[cols].to_csv('./data/clean/2021-W29-output.csv', index=False)