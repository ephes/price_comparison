#!/anaconda/bin/python3

import os
import re
import sys

import requests

from io import StringIO
from joblib import Memory
from joblib import Parallel, delayed

from urllib.parse import unquote
from xml.etree.ElementTree import ParseError

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
    content = None
    try:
        r = requests.get(shopinfo_url, timeout=10)
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


def shopinfo_url_generator():
    num = 0
    #for elmar_url in generate_elmar_urls(4633, 50):
    for elmar_url in generate_elmar_urls(100, 50):
        shopinfo_urls = get_shopinfo_urls_from_page(elmar_url)
        for shopinfo_url in shopinfo_urls:
            # print('shopinfo_url num: {}'.format(num))
            num += 1
            yield shopinfo_url


def get_shopinfo(shopinfo_url):
    shopinfo = None
    shopinfo_xml = get_shopinfo_xml_from_url(shopinfo_url)
    #print(shopinfo_xml)
    if shopinfo_xml is not None:
        try:
            shopinfo = Shopinfo(shopinfo_xml)
        except ParseError:
            print('parse_error: {}'.format(shopinfo_url))
        except AttributeError:
            print('root node is None: {}'.format(shopinfo_url))
    return shopinfo


def get_shops_with_ean():
    all_shopinfo = Parallel(n_jobs=8)(
        delayed(get_shopinfo)(su) for su in shopinfo_url_generator())
    num = 0
    ean_shopinfos = []
    shopinfo_nums = []
    for shopinfo in all_shopinfo:
        if shopinfo is not None and shopinfo.has_ean:
            try:
                if shopinfo.product_count is not None:
                    num += shopinfo.product_count
                    shopinfo_nums.append((shopinfo.product_count, shopinfo.name))
                    ean_shopinfos.append(shopinfo)
                print('name: {} products: {} csv_url: {} delimiter: <{}> lineend: <{}>'.format(shopinfo.name, shopinfo.product_count, shopinfo.csv_url, shopinfo.csv_delimiter, shopinfo.csv_lineend))
            except ParseError:
                pass
    for pnum, name in sorted(shopinfo_nums):
        print(pnum, name)
    print(num)
    return ean_shopinfos


def fetch_feed_csv(shopinfo):
    shopinfo.download_feed_csv()
    return shopinfo


def main(args):
    ean_shopinfos = get_shops_with_ean()
    print(len(ean_shopinfos))
    ean_shopinfos = [fetch_feed_csv(shopinfo) for shopinfo in ean_shopinfos]


if __name__ == '__main__':
    main(sys.argv[1:])
