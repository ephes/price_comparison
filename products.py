import pandas as pd


def convert_price(price):
    price = str(price)
    if ',' in price:
        if '.' in price:
            price = price.replace('.', '')
        price = price.replace(',', '.')
        
    if ' EUR' in price:
        price = price.replace(' EUR', '')

    try:
        return float(price)
    except ValueError:
        #print('<{}>'.format(price))
        return None


def get_products(csv_name='products.csv'):
    products = pd.read_csv(
        csv_name, error_bad_lines=False, converters={'price': convert_price},
        low_memory=False)
    products = products.drop('Unnamed: 0', 1)
    products = products[~products.price.isnull()]
    return products
