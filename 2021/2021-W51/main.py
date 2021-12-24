import pandas as pd

df = pd.read_csv('./data/raw/2021W51 Input.csv', parse_dates=['Order Date'])

if __name__ == '__main__':

    # split order_id and store name
    df[['Store', 'OrderID']] = df['OrderID'].str.split('-', expand=True)
    df['OrderID'] = df['OrderID'].astype(int)

    # fill return state with 1 or 0 (not null / null)
    df['Return State'] = df['Return State'].notnull().astype('int')
    df['Unit Price'] = df['Unit Price'].str.replace('£', '').astype(float)

    # store table
    stores = (df
        .groupby('Store', as_index=False)['Order Date'].min()
        .rename(columns={'Order Date': "First Order"})
        .sort_values(by=['First Order', 'Store'])
        .reset_index(drop=True)
    )

    stores['StoreID'] = stores.index + 1
    cols = ['StoreID', 'Store', 'First Order']
    stores[cols].to_csv('./data/clean/store-table.csv', index=False)

    # customer table
    customers = (df
        .groupby('Customer').agg({"Return State": 'sum', "OrderID": 'size', "Order Date": 'min'})
        .rename(columns={'OrderID': 'Number of Orders', "Order Date": 'First Order'})
        .sort_values(by=['First Order', 'Customer'])
        .reset_index()
    )

    customers['Return %'] = customers['Return State'] / customers['Number of Orders']
    customers['CustomerID'] = customers.index + 1

    cols = ['CustomerID', 'Customer', 'Return %', 'Number of Orders', 'First Order']
    customers[cols].to_csv('./data/clean/customer-table.csv', index=False)

    # products table
    products = (df
        .groupby(['Product Name', 'Category', 'Sub-Category', 'Unit Price'], as_index=False)['Order Date'].min()
        .rename(columns={"Order Date": "First Sold"})
        .sort_values(by=['First Sold', 'Product Name'])
        .reset_index(drop=True)
    )

    products['ProductID'] = products.index + 1
    cols = ['ProductID', 'Category', 'Sub-Category', 'Product Name', 'Unit Price', 'First Sold']
    products[cols].to_csv('./data/clean/products-table.csv', index=False)

    # fact table
    fact_table = (df
        .merge(products[['Product Name', 'ProductID']], on='Product Name', how='left')
        .merge(customers[['Customer', 'CustomerID']], on='Customer', how='left')
        .merge(stores[['Store', 'StoreID']], on='Store', how='left')
        .assign(Sales=df['Unit Price'] * df['Quantity'])
        .drop(columns=['Customer', 'Store', 'Product Name'])
        .rename(columns={"Return State": "Returned"})
    )

    cols = ['StoreID', 'CustomerID', 'OrderID', 'Order Date', 'ProductID', 'Returned', 'Quantity', 'Sales']
    fact_table[cols].to_csv('./data/clean/fact-table.csv', index=False)