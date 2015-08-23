#!/anaconda/bin/python3

import os
import re
import sys

import requests

from io import StringIO
from joblib import Memory
from urllib.parse import unquote

from shopinfo import Shopinfo

memory = Memory(cachedir='cache')


def generate_elmar_urls(shopcount, blocksize):
    url_tmpl = 'http://elektronischer-markt.de/nav?page={}&blocksize={}&dest=search.shoplist'
    pages = int((shopcount / blocksize) + 1)
    for page in range(1, pages + 1):
        yield url_tmpl.format(page, blocksize)


def get_shopinfo_url_from_line(line, pattern):
    line = unquote(line)
    m = pattern.search(line)
    if m is not None:
        url = m.group('url')
        parts = url.split('/')
        parts[-1] = 'shopinfo.xml'
        url = '/'.join(parts)
        return url


@memory.cache
def get_shopinfo_urls_from_page(elmar_url):
    line_pattern = re.compile('a.target.*counter.*home')
    url_pattern = re.compile(r'redirect=(?P<url>http.*)&amp;rid=2"')
    shopinfo_urls = []
    r = requests.get(elmar_url)
    print(r.status_code)
    for line in StringIO(r.content.decode('utf8')):
        line = line.rstrip()
        m = line_pattern.search(line)
        if m is not None:
            shopinfo_url = get_shopinfo_url_from_line(line, url_pattern)
            shopinfo_urls.append(shopinfo_url)
    return shopinfo_urls


@memory.cache
def get_shopinfo_xml_from_url(shopinfo_url):
    r = requests.get(shopinfo_url)
    return r.content


def main(args):
    for elmar_url in generate_elmar_urls(4633, 50):
        shopinfo_urls = get_shopinfo_urls_from_page(elmar_url)
        for shopinfo_url in shopinfo_urls:
            shopinfo_xml = get_shopinfo_xml_from_url(shopinfo_url)
            print(shopinfo_xml)
            shopinfo = Shopinfo(shopinfo_xml)
            print(shopinfo.name)
        #r = requests.get(url)
        #fname = shopinfo_url.replace('http://', '').replace('/', '_')
        #with open('shopinfo/{}'.format(fname), 'w') as sfile:
        #    sfile.write(r.content)
        #except Exception:
        #    print('broken shopinfo: {}'.format(shopinfo_url))


if __name__ == '__main__':
    main(sys.argv[1:])
