import os
import re
import requests
import pandas as pd
import xml.etree.ElementTree as ET

from hashlib import md5


class Shopinfo(object):
    def __init__(self, url, feed_dir='feeds',
                 shopinfo_dir='shopinfos'):
        self.url = url
        self.feed_dir = feed_dir
        self.shopinfo_dir = shopinfo_dir

    def _get_hash(self):
        return md5(self.url.encode('utf8')).hexdigest()

    @property
    def feed_path(self):
        feed_name = '{}.csv'.format(self._get_hash())
        feed_path = os.path.join(self.feed_dir, feed_name)
        return feed_path

    @property
    def shopinfo_path(self):
        shopinfo_name = '{}.csv'.format(self._get_hash())
        shopinfo_path = os.path.join(self.shopinfo_dir, shopinfo_name)
        return shopinfo_path

    def _get_shopinfo_from_url(self):
        content = None
        try:
            r = requests.get(self.url, timeout=10)
            if r.status_code == requests.codes.ok:
                if len(r.content) > 0:
                    content = r.content
        except requests.exceptions.ConnectionError:
            print('connection aborted: {}'.format(shopinfo_url))
        except requests.exceptions.InvalidSchema:
            print('invalid schema: {}'.format(shopinfo_url))
        except requests.exceptions.TooManyRedirects:
            print('too many redirects: {}'.format(shopinfo_url))
        except requests.exceptions.InvalidURL:
            print('invalid url: {}'.format(shopinfo_url))
        return content

    @property
    def shopinfo_str(self):
        if not os.path.exists(self.shopinfo_path):
            if not os.path.exists(self.shopinfo_dir):
                os.makedirs(self.shopinfo_dir)
            shopinfo_str = self._get_shopinfo_from_url()
            with open(self.shopinfo_path, 'wb') as f:
                f.write(shopinfo_str)
        else:
            shopinfo_str = None
            with open(self.shopinfo_path, 'rb') as f:
                shopinfo_str = f.read()
        return shopinfo_str

    @property
    def root(self):
        if not hasattr(self, '_root'):
            self._root = ET.fromstring(self.shopinfo_str)
        return self._root

    @property
    def encoding(self):
        lines = self.shopinfo_str.split(b'\n')
        p = re.compile(b'encoding="(?P<encoding>.*?)"')
        m = p.search(lines[0])
        print(lines[0], m)
        if m is not None:
            return m.group('encoding').decode()
        return None
        
    @property
    def name(self):
        return self.root.find('Name').text
    
    @property
    def shop_url(self):
        return self.root.find('Url').text
    
    @property
    def tabular(self):
        return self.root.find('.//Tabular')

    @property    
    def mappings(self):
        cols = []
        mappings = self.tabular.find('Mappings')
        if mappings is None:
            # sometimes mappings is None
            return cols
        for mapping in mappings:
            column_num = int(mapping.attrib['column'])
            column_name = mapping.attrib['columnName']
            column_type = mapping.attrib['type']
            cols.append((column_num, column_name, column_type))
        return cols

    @property
    def _column_lookup(self):
        return {k: v for num, k, v in self.mappings}

    @property
    def has_ean(self):
        ean = False
        try:
            for count, name, ctype in self.mappings:
                if ctype == 'ean':
                    ean = True
                if name.lower() == 'ean':
                    ean = True
        except AttributeError:
            pass
        return ean
    
    @property
    def csv_url(self):
        csv_url = None
        try:
            csv_url = self.tabular.find('CSV/Url').text
        except AttributeError:
            pass
        return csv_url

    @property
    def csv_delimiter(self):
        schars = self.tabular.find('CSV/SpecialCharacters').attrib
        delimiter = schars['delimiter']
        if delimiter == '[tab]':
            delimiter = '\t'
        return delimiter

    @property
    def csv_lineend(self):
        schars = self.tabular.find('CSV/SpecialCharacters').attrib
        return schars.get('lineend')

    @property
    def product_count(self):
        product_count = None
        try:
            product_count = int(
                self.root.find(".//Categories/TotalProductCount").text
            )
        except AttributeError:
            pass
        return product_count
    
    @property
    def categories(self):
        categories = []
        for category in self.root.findall(".//Categories/Item"):
            name = category.find('Name').text
            count = category.find('ProductCount').text
            mapping = category.find('Mapping').text
            categories.append((name, mapping, int(count)))
        return categories

    def download_feed_csv(self):
        if os.path.exists(self.feed_path):
            return self.feed_path
        if not os.path.exists(self.feed_dir):
            os.makedirs(self.feed_dir)
        r = requests.get(self.csv_url, stream=True)
        with open(self.feed_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
        return self.feed_path

    def get_dataframe(self):
        df = pd.read_csv(self.feed_path, delimiter=self.csv_delimiter,
                         encoding=self.encoding)
        df.rename(columns=self._column_lookup, inplace=True)
        return df
