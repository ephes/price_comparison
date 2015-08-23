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
        r = requests.get(shopinfo_url)
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
    for elmar_url in generate_elmar_urls(4633, 50):
        shopinfo_urls = get_shopinfo_urls_from_page(elmar_url)
        for shopinfo_url in shopinfo_urls:
            print('shopinfo_url num: {}'.format(num))
            num += 1
            yield shopinfo_url


def get_shopinfo(shopinfo_url):
    shopinfo = None
    shopinfo_xml = get_shopinfo_xml_from_url(shopinfo_url)
    #print(shopinfo_xml)
    if shopinfo_xml is not None:
        try:
            shopinfo = Shopinfo(shopinfo_xml)
            if shopinfo.has_ean:
                print('has_ean: {}'.format(shopinfo_url))
            #print(shopinfo.name)
            #print(shopinfo.mappings)
        except ParseError:
            print('parse_error: {}'.format(shopinfo_url))
        except AttributeError:
            print('root node is None: {}'.format(shopinfo_url))
    return shopinfo


def main(args):
    Parallel(n_jobs=3)(delayed(get_shopinfo)(su)
                        for su in shopinfo_url_generator())


if __name__ == '__main__':
    main(sys.argv[1:])
