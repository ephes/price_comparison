import os
import re
import requests
import xml.etree.ElementTree as ET

from hashlib import md5


class Shopinfo(object):
    def __init__(self, shopinfo, feed_dir='feeds'):
        self.root = ET.fromstring(shopinfo)
        if self.root is None:
            return None
        self.feed_dir = feed_dir
        self.feed_path = None
        self.shopinfo_str = shopinfo

    @property
    def encoding(self):
        lines = self.shopinfo_str.split(b'\n')
        p = re.compile(b'encoding="(?P<encoding>.*)"')
        m = p.search(lines[0])
        print(lines[0], m)
        if m is not None:
            return m.group('encoding').decode()
        return None
        
    @property
    def name(self):
        return self.root.find('Name').text
    
    @property
    def url(self):
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
        return schars['delimiter']

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
        if not os.path.exists(self.feed_dir):
            os.makedirs(self.feed_dir)
        feed_name = '{}.csv'.format(
            md5(self.csv_url.encode('utf8')).hexdigest())
        feed_path = os.path.join(self.feed_dir, feed_name)
        r = requests.get(self.csv_url, stream=True)
        with open(feed_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
        self.feed_path = feed_path
        return self.feed_path
