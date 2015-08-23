import xml.etree.ElementTree as ET


class Shopinfo(object):
    def __init__(self, shopinfo):
        self.root = ET.fromstring(shopinfo)
        
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
        for mapping in mappings:
            column_num = int(mapping.attrib['column'])
            column_name = mapping.attrib['columnName']
            column_type = mapping.attrib['type']
            cols.append((column_num, column_name, column_type))
        return cols

    @property
    def has_ean(self):
        ean = False
        for count, name, ctype in self.mappings:
            if ctype == 'ean':
                ean = True
            if name.lower() == 'ean':
                ean = True
        return ean
    
    @property
    def csv_url(self):
        return self.tabular.find('CSV/Url').text
    
    @property
    def product_count(self):
        return int(self.root.find(".//Categories/TotalProductCount").text)
    
    @property
    def categories(self):
        categories = []
        for category in self.root.findall(".//Categories/Item"):
            name = category.find('Name').text
            count = category.find('ProductCount').text
            mapping = category.find('Mapping').text
            categories.append((name, mapping, int(count)))
        return categories
