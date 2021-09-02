import pandas as pd

def get_min_max(size):
    if size.endswith('2'):
        size = int(size.replace('cm2', ''))
        return pd.Series([size, size])
    else:
        size = size.replace('cm', '').split(' x ')
        size = [int(x) for x in size]
        size.sort()
        return pd.Series([size[1], size[0]])
    
def normalize_frame(size):
    if ('cm' in size) and (size.endswith('2')):
        size = int(size.replace('cm2', ''))
        return pd.Series([size, size]) 
    elif ('cm' in size) and ('x' in size):
        size = size.replace('cm', '').split(' x ')
        size = [int(x) for x in size]
        size.sort()
        return pd.Series([size[1], size[0]])
    else:
        size = size.replace('"', '').split(' x ')
        size = [int(x) * 2.54 for x in size]
        size.sort()
        return pd.Series([size[1], size[0]])



if __name__ == '__main__':
    # read in picture data and get max/min size
    pic = pd.read_excel('./data/raw/Pictures Input.xlsx', sheet_name=0)
    pic[['max_size', 'min_size']] = pic['Size'].apply(get_min_max)
    pic = pic.drop(columns='Size')

    # read in frame data and get max/min size
    frames = pd.read_excel('./data/raw/Pictures Input.xlsx', sheet_name=1)
    frames[['max_size', 'min_size']] = frames['Size'].apply(normalize_frame)
    frames = frames.rename(columns={'Size': 'Frame'})

    # merge all frames to each picture
    df = pic.merge(frames, how='cross', suffixes=['_pic', '_frame'])

    # get size difference
    df['max_diff'] = df['max_size_pic'] - df['max_size_frame']
    df['min_diff'] = df['min_size_pic'] - df['min_size_frame']
    df['total_diff'] = df['max_diff'] + df['min_diff']

    # filter out frames too small & select least excess
    df = df[(df[['max_diff', 'min_diff']] > 0).any(axis=1) == False]
    df = df.loc[(df.groupby('Picture')['total_diff'].idxmax())]

    # get wanted columns & save output
    df = df[['Picture', 'Frame', 'max_size_pic', 'min_size_pic']]
    df.to_csv('./data/clean/picture_frames.csv')