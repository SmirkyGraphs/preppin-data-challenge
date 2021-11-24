import pandas as pd

file = pd.ExcelFile('./data/raw/Bookshop.xlsx')

# detail tables
books = file.parse('Book')
auth = file.parse('Author')
info = file.parse('Info')
edit = file.parse('Edition')
pub = file.parse('Publisher')
series = file.parse('Series')

# aggregate tables
award = file.parse('Award')
chk = file.parse('Checkouts')
rate = file.parse('Ratings')

# combine sales tables
frames = []
for quarter in file.sheet_names[-4:]:
    data = file.parse(quarter)
    frames.append(data)

sales = pd.concat(frames)

if __name__ == '__main__':

    # fix info book_id field
    info['BookID'] = info['BookID1'] + info['BookID2'].astype(str)
    info = (info
        .assign(BookID= info['BookID1'] + info['BookID2'].astype(str))
        .drop(columns=['BookID1', 'BookID2'])
    )

    # number of awards by book title
    avg_award = award.groupby('Title').size().reset_index(name='Number of Awards')

    # num of months checked out & total checkouts
    checkout_agg = (chk
        .groupby('BookID')['Number of Checkouts'].agg(['size', 'sum'])
        .rename(columns={'size': 'Num of Months Checked Out', 'sum': 'Total Checkouts'})
        .reset_index()
    )

    # num of reviews, unique reviewers and avg rating by book
    rate_agg = (rate
        .groupby('BookID', as_index=False)['ReviewID'].size()
        .merge(rate.groupby('BookID', as_index=False)['Rating'].mean())
        .merge(rate.groupby('BookID', as_index=False)['ReviewerID'].nunique())
        .rename(columns={'size': 'Num of Reviews', 'Rating': 'Avg Rating', 'ReviewerID': 'Num of Reviewers'})
    )

    # combine all datasets into sales
    final_df = (sales
        .merge(edit, on='ISBN', how='left')
        .merge(pub, on='PubID', how='left')
        .merge(books, on='BookID', how='left')
        .merge(auth, on='AuthID', how='left')
        .merge(info, on='BookID', how='left')
        .merge(series, on='SeriesID', how='left')
        .merge(avg_award, on='Title', how='left')
        .merge(checkout_agg, on='BookID', how='left')
        .merge(rate_agg, on='BookID', how='left')
    )

    # save final combined data to clean output folder
    final_df.to_csv('./data/clean/2021-W46-output.csv', index=False)