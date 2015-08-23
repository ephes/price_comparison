#!/anaconda/bin/python3

import os
import re
import sys

import requests

from joblib import Memory
from urllib.parse import unquote

cache = Memory(cachedir='cache')


def generate_urls(shopcount, blocksize):
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


def main(args):
    #for url in generate_urls(4633, 50):
    #    print(url)
    line_pattern = re.compile('a.target.*counter.*home')
    url_pattern = re.compile(r'redirect=(?P<url>http.*)&amp;rid=2"')
    with open('page_1', 'r') as content:
        for line in content:
            line = line.rstrip()
            m = line_pattern.search(line)
            if m is not None:
                shopinfo_url = get_shopinfo_url_from_line(line, url_pattern)
                print(shopinfo_url)
                r = requests.get(shopinfo_url)
                fname = shopinfo_url.replace('http://', '').replace('/', '_')
                with open('shopinfo/{}'.format(fname), 'w') as sfile:
                    sfile.write(r.content)
                #except Exception:
                #    print('broken shopinfo: {}'.format(shopinfo_url))


if __name__ == '__main__':
    main(sys.argv[1:])
