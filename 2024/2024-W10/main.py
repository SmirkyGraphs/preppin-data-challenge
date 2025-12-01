import polars as pl

# while checking loyalty data, 12123 Bronze members have 5% discount while 583 have none (sample id 1003741)
# unsure if intended, the sample output provided keeps them this way so they are left alone

data = pl.read_excel('./data/raw/2024W10 Input.xlsx', sheet_id=0)

products = (data['Product Table']
    .with_columns(
        pl.col('Product_Size').fill_null(pl.col('Pack_Size'))
    )
    .drop(['Pack_Size'])
)

loyalty = (data['Loyalty Table']
    .with_columns(pl.col('Customer_Name')
        .str.split(', ')
        .list.reverse() 
        .list.join(" ")  
        .str.to_titlecase()
    )
    .with_columns(pl
        .when(pl.col("Loyalty_Tier").str.to_lowercase().str.starts_with("b"))
        .then(pl.lit('Bronze'))
        .when(pl.col("Loyalty_Tier").str.to_lowercase().str.starts_with("s"))
        .then(pl.lit('Silver'))
        .when(pl.col("Loyalty_Tier").str.to_lowercase().str.starts_with("g"))
        .then(pl.lit('Gold'))
        .alias('Loyalty_Tier')
    )
    .with_columns(
        pl.col('Loyalty_Discount').str.replace("%", "").cast(pl.Float64) / 100
    )
)

df = (data['Transaction Data']
    .with_columns([
        pl.col('Transaction_Date').str.to_date(format="%a, %B %d, %Y"),
    ])
    .filter(pl.col('Transaction_Date').dt.year() > 2022)
    .sort('Transaction_Date')
    .with_columns([
        pl.col('Cash_or_Card').cast(pl.Utf8).replace({"1": "Card", "2": "Cash"}),
        pl.col('Product_ID').str.split('-').list.get(0).alias('Product_Type'),
        pl.col('Product_ID').str.split('-').list.get(1).str.replace("_", ' ').alias('Product_Scent'),
        pl.col('Product_ID').str.split('-').list.get(2).alias('Product_Size'),
    ])
    .upsample( # fills in missing dates
        time_column='Transaction_Date',
        every='1d'
    )
    .join(products, how='left', on=['Product_Type', 'Product_Scent', 'Product_Size'])
    .join(loyalty, how='left', on=['Loyalty_Number'])
    .with_columns([
        pl.col('Sales_Before_Discount').truediv(pl.col('Selling_Price')).alias('Quantity'),
        pl.col('Sales_Before_Discount').mul(1 - pl.col('Loyalty_Discount')).alias('Sales_After_Discount')
    ])
    .with_columns(
        (pl.col('Sales_After_Discount') - (pl.col('Unit_Cost') * pl.col('Quantity'))).round(2).alias('Profit')
    )
    .rename({'Transanction_Number': 'Transaction_Number'})
    .rename(lambda col_name: col_name.replace("_", " "))
    .select([
        'Transaction Date',
        'Transaction Number',
        'Product Type',
        'Product Scent',
        'Product Size',
        'Cash or Card',
        'Loyalty Number',
        'Customer Name',
        'Loyalty Tier',
        'Loyalty Discount',
        'Quantity',
        'Sales Before Discount',
        'Sales After Discount',
        'Profit',
    ])
)

df.write_csv('./data/clean/2024-W10-output.csv')