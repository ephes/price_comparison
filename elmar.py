#!/anaconda/bin/python3

import os
import re

from io import StringIO
from queue import Queue
from threading import Thread
from urllib.parse import unquote

import requests


class ElmarPage:
    url_tmpl = (
        'http://elektronischer-markt.de/nav?page={}'
        '&blocksize={}&dest=search.shoplist'
    )
    line_pattern = re.compile('a.target.*counter.*home')
    url_pattern = re.compile(r'redirect=(?P<url>http.*)&amp;rid=2"')

    def __init__(self, page, blocksize, elmar_dir='elmar'):
        self.page = page
        self.blocksize = blocksize
        self.elmar_dir = elmar_dir
        self.path = os.path.join(
            self.elmar_dir, '{}_{}.txt'.format(self.page, self.blocksize))
        self.url = self.url_tmpl.format(page, blocksize)

    def get_shopinfo_url_from_line(self, line):
        line = unquote(line)
        m = self.url_pattern.search(line)
        if m is not None:
            url = m.group('url')
            parts = url.split('/')
            parts[-1] = 'shopinfo.xml'
            url = '/'.join(parts)
            return url

    def fetch_shopinfo_urls(self):
        shopinfo_urls = []
        r = requests.get(self.url)
        for line in StringIO(r.content.decode('utf8')):
            line = line.rstrip()
            m = self.line_pattern.search(line)
            if m is not None:
                shopinfo_url = self.get_shopinfo_url_from_line(line)
                shopinfo_urls.append(shopinfo_url)
        return shopinfo_urls

    def download_page(self):
        if not os.path.exists(self.elmar_dir):
            os.makedirs(self.elmar_dir)
        shopinfo_urls = self.fetch_shopinfo_urls()
        with open(self.path, 'w') as f:
            for shopinfo_url in shopinfo_urls:
                f.write('{}\n'.format(shopinfo_url))

    @property
    def shopinfo_urls(self):
        if not os.path.exists(self.path):
            self.download_page()
        shopinfo_urls = []
        with open(self.path, 'r') as f:
            for line in f:
                shopinfo_urls.append(line.rstrip())
        return shopinfo_urls


class ElmarWorker(Thread):
    def __init__(self, job_queue, result_queue):
        Thread.__init__(self)
        self.job_queue = job_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            elmar_page = self.job_queue.get()
            self.result_queue.put(elmar_page.shopinfo_urls)
            self.job_queue.task_done()


def start_workers(job_queue, result_queue, num=10):
    for _ in range(num):
        worker = ElmarWorker(job_queue, result_queue)
        worker.daemon = True
        worker.start()


def get_shopinfo_urls(shopcount, blocksize):
    job_queue, result_queue = Queue(), Queue()
    start_workers(job_queue, result_queue)
    pages = int((shopcount / blocksize) + 1)
    work_count = 0
    for page in range(1, pages + 1):
        job_queue.put(ElmarPage(page, blocksize))
        work_count += 1

    results = []
    while work_count > 0:
        results.extend(result_queue.get())
        print(work_count)
        work_count -= 1

    job_queue.join()
    return results
