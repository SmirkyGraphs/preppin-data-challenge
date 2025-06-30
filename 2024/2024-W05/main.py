import polars as pl

sales = pl.read_csv('./data/raw/prep air ticket sales.csv', try_parse_dates=True)
routes = pl.read_csv('./data/raw/prep air 2024 flights.csv', try_parse_dates=True)
customers = pl.read_csv('./data/raw/prep air customers.csv', try_parse_dates=True)

# Create a dataset that gives all the customer details for booked flights in 2024
# Make sure the output also includes details on the flights origin and destination
master = (sales
    .join(customers, on='Customer ID', how='left')
    .join(routes, on=['Flight Number', 'Date'], how='left')
)

# # Create a dataset that allows Prep Air to identify which flights have not yet been booked in 2024
unbooked_flights = (routes
    .join(sales, on=['Date', 'Flight Number'], how='left')
    .filter(pl.col('Customer ID').is_null())
    .drop(['Customer ID', 'Ticket Price'])
    .sort(['Date', 'Flight Number'])
)

# # Create a dataset that shows which customers have yet to book a flight with Prep Air in 2024
unbooked_customers = (customers
    .join(sales, on='Customer ID', how='left')
    .with_columns([
        pl.col('Date').max().over('Customer ID').alias('max_date')
    ])
    .filter(pl.col('max_date').is_null())
    .drop(['max_date', 'Date', 'Flight Number', 'Ticket Price'])
)

master.write_csv('./data/clean/2024-W05-output-1.csv')
unbooked_flights.write_csv('./data/clean/2024-W05-output-2.csv')
unbooked_customers.write_csv('./data/clean/2024-W05-output-3.csv')