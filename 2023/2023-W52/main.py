import polars as pl
import json

# load external renamer from json file (i decided to go with broad categories a total of 7)
with open('./data/clean/name_map.json', 'r') as f:
    _json = json.load(f)
    renamer = {x: k for k, v in _json.items() for x in v}

# strip and uppercase to normalize themes
split_themes = lambda x: [y.strip().upper() for y in x]

if __name__ == '__main__':

    df = (pl
        .scan_csv("./data/raw/Preppin' Themes.csv")
        .drop('Watch out for...')
        .with_columns([
            pl.col('Themes').str.split(',').map_elements(split_themes)
        ])
        .explode('Themes')
        .filter(pl.col("Themes") != '')
        .with_columns([
            pl.col('Themes').replace(renamer)
        ])   
        .collect()
        .pivot(
            values='Themes',
            index="Themes", 
            columns="Level", 
            aggregate_function="count"
        )
        .fill_null(0)
        .with_columns([
            pl.sum_horizontal(pl.all().exclude('Themes')).alias('total')
        ])  
    )

    df.write_csv('./data/clean/2023-W51-output.csv')
