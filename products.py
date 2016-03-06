import pandas as pd

from random import sample
from collections import defaultdict


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
    """ get products dataframe from csv"""
    products = pd.read_csv(
        csv_name, error_bad_lines=False, converters={'price': convert_price},
        low_memory=False)
    products = products.drop('Unnamed: 0', 1)
    products = products[~products.price.isnull()]
    return products


def get_products_from_different_shops(
    csv_name='products.csv', n=5, sample_size=10):
    """ select only eans which appear in more than n shops"""
    products = get_products(csv_name=csv_name)
    shops_for_ean = defaultdict(set)

    for ean, shop in zip(products.ean, products.shop):
        shops_for_ean[ean].add(shop)

    good_eans = list(
        {ean for ean, shops in shops_for_ean.items() if len(shops) > n})
    sample_eans = sample(good_eans, sample_size)
    sample_products = products[products.ean.isin(pd.Series(sample_eans))]
    return sample_products
