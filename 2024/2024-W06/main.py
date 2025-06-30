import polars as pl

bracket_1 = 12_570
bracket_2 = 50_270
bracket_3 = 125_140

def get_max_bracket(salary):
    if salary <= bracket_1:
        return "0% rate"
    
    if salary <= bracket_2:
        return "20% rate"

    if salary <= bracket_3:
        return "40% rate"

    return "45% rate"

def tax_calculation(salary):
    rate_20, rate_40, rate_45 = 0.0, 0.0, 0.0

    if salary <= bracket_2:
        rate_20 = 0.2 * (salary - bracket_1)
    else:
        rate_20 = 0.2 * (bracket_2 - bracket_1)

    if salary <= bracket_3:
        rate_40 = 0.4 * (salary - bracket_2)
    else:
        rate_40 = 0.4 * (bracket_3 - bracket_2)

    if salary > bracket_3:
        rate_45 = 0.45 * (salary - bracket_3)

    return {
        "total tax paid": rate_20 + rate_40 + rate_45,
        "20% rate paid": rate_20, 
        "40% rate paid": rate_40, 
        "45% rate paid": rate_45
    }

df = (pl
    .read_csv('./data/raw/PD 2024 Wk 6 Input.csv', row_index_name='row_num')
    .filter(pl.col('row_num') == pl.col('row_num').max().over('StaffID'))
    .unpivot(index=['row_num', 'StaffID'])
    .group_by('StaffID').agg([
        pl.col('value').sum()
    ])
    .with_columns([
        pl.col("value").map_elements(get_max_bracket, return_dtype=pl.String).alias("max_tax_rate"),
        pl.col("value").map_elements(tax_calculation, return_dtype=pl.Struct).alias("taxes")
    ])
    .unnest("taxes")
    .sort('StaffID')
)

df.write_csv('./data/clean/2024-W06-output.csv')