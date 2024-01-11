import polars as pl
from dateutil import parser

def parse_dates(date):
    return parser.parse(date)

# get students first + last name and id
students = (pl
    .scan_csv('./data/raw/Student_LookupTable.csv')
    .with_columns(
        full_name = pl.col("first_name") + " " + pl.col("last_name")
    )
    .drop(['first_name', 'last_name'])
)

# read the excel file with all sheets as a dict
sheets = pl.read_excel('./data/raw/Student_Grades_input.xlsx', sheet_id=0)

# loop over sheets, remove nulls, add term and fix messy date format
terms = []
for sheet in sheets.keys():
    if not sheet.startswith('Term'):
        continue

    cols = sheets[sheet].columns[2:]
    terms.append(
        sheets[sheet]
        .drop_nulls()
        .with_columns(
            gpa = pl.concat_list(cols).list.mean(),
            term = pl.lit(sheet),
            Date = (
                pl.when(pl.col("Date") == '2021-06')
                  .then(pl.lit("2021-06-16"))
                  .otherwise(pl.col('Date'))
                  .map_elements(parse_dates)
            )
        )
        .drop(cols)
    )

# combine all terms into 1 dataframe and add rolling avg
terms = (
    pl.concat(terms)
    .sort(["id"], descending=False)
    .with_columns(
        rolling_gpa = (
            pl.col("gpa").rolling_mean(window_size=3).over('id').round(2)
        )
    )
)

terms = (terms
    .join(students.collect(), how='left', on='id')
    .sort('full_name', descending=True)
    .with_row_count(name='rank', offset=1)
    .select(['rank', 'id', 'full_name', 'term', 'Date', 'gpa', 'rolling_gpa'])
)

top_pct = (terms
    .filter(pl.col('term').is_in(['Term 3', 'Term 6']))
    .sort('rolling_gpa', descending=True)
    .filter(pl.col("rolling_gpa") >= pl.col("rolling_gpa").quantile(0.98))

)


terms.write_csv('./data/clean/2023-W47-output-1.csv')
top_pct.write_csv('./data/clean/2023-W47-output-2.csv')