# Class to stream stock price data from Google finance
import re
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, connection

import requests
from fake_useragent import UserAgent

from base import Producer, Item
from extensions.proxy import ProxySnatcher


class FinanceWorker(Process):
    def __init__(self, port=6009, send_every=60):
        super(FinanceWorker, self).__init__()
        self.topics = Producer.load_query_dictionary('query_topics1.txt')
        self.port = port
        self.send_every = send_every
        self.running = True

    @staticmethod
    def extract_stock(html_text):
        """Scrapes stock price from html_text from google finance"""
        stock_price = 0.
        tmp = re.search(r'id="ref_(.*?)">(.*?)<', html_text)
        if tmp:
            stock_price = eval(tmp.group(2).replace(',', ''))
        return stock_price

    def finance_search(self, query, proxy, user_agent, proxy_thread):
        """Query recent stock price for query using provided user agent and proxy"""
        search_url = "http://www.google.com/finance?&q=" + query
        resp, stock_price = '', 0.0
        ex = False
        while not resp:
            try:
                resp = requests.get(search_url, proxies=proxy if not ex else proxy_thread.random('article', True),
                                    headers={'User-Agent': user_agent})
                stock_price = self. extract_stock(resp.text)
            except Exception as e:
                print('Finance', e)
                ex = True
        return Item(content=stock_price, topic=query, source='stock')

    def run(self):
        l = connection.Listener(('localhost', self.port), authkey=b'veryscrape')
        outgoing = l.accept()
        proxy_thread = ProxySnatcher(len(self.topics),
                                     **{'minDownloadSpeed': '100',
                                        'protocol': 'http',
                                        'allowsHttps': 1,
                                        'allowsUserAgentHeader': 1,
                                        'allowsCustomHeaders': 1})
        proxy_thread.start()
        fua = UserAgent()
        proxy_thread.wait_for_proxies()
        pool = ThreadPoolExecutor(len(self.topics))
        while self.running:
            start = time.time()
            for query in self.topics:
                proxy = proxy_thread.random('article', True)
                # Submit finance search for each query with random proxy
                future = pool.submit(self.finance_search, query, proxy, fua.random, proxy_thread)
                future.add_done_callback(lambda f: outgoing.send(f.result()))
            time.sleep(max(0, self.send_every - (time.time() - start)))
