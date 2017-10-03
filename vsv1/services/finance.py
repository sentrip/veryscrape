# Class to stream stock price data from Google finance
import re
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, connection

import requests
from fake_useragent import UserAgent

from vsv1.base import Producer, Item
from vsv1.extensions.proxy import random_proxy_sync


class FinanceWorker(Process):
    def __init__(self, port=6009, send_every=60):
        super(FinanceWorker, self).__init__()
        self.topics = Producer.load_query_dictionary('query_topics.txt')
        self.port = port
        self.send_every = send_every
        self.proxy_params = {'minDownloadSpeed': '150', 'protocol': 'http',
                             'allowsHttps': 1, 'allowsUserAgentHeader': 1, 'maxConnectTime': '1'}
        self.running = True

    @staticmethod
    def extract_stock(html_text):
        """Scrapes stock price from html_text from google finance"""
        stock_price = -1
        tmp = re.search(r'id="ref_(.*?)">(.*?)<', html_text)
        if tmp:
            stock_price = eval(tmp.group(2).replace(',', ''))
        return stock_price

    def finance_search(self, query, proxy, user_agent):
        """Query recent stock price for query using provided user agent and proxy"""
        search_url = "http://www.google.com/finance?&q=" + query
        resp, stock_price = '', -1
        ex = False
        count = 0
        while not resp and count < 100:
            try:
                resp = requests.get(search_url, proxies=proxy if not ex else random_proxy_sync(**self.proxy_params),
                                    headers={'User-Agent': user_agent}, timeout=3)
                stock_price = self. extract_stock(resp.text)
            except (TimeoutError, requests.exceptions.ConnectionError):
                ex = True
                count += 1
            except Exception as e:
                print('Finance', repr(e))
                ex = True
                count += 1
        return Item(content=stock_price, topic=query, source='stock')

    def run(self):
        l = connection.Listener(('localhost', self.port), authkey=b'vs')
        outgoing = l.accept()
        fua = UserAgent()
        pool = ThreadPoolExecutor(len(self.topics))
        while self.running:
            start = time.time()
            for query in self.topics:
                proxy = random_proxy_sync(**self.proxy_params)
                # Submit finance search for each query with random proxy
                future = pool.submit(self.finance_search, query, proxy, fua.random)
                future.add_done_callback(lambda f: outgoing.send(f.result()))
            time.sleep(max(0, self.send_every - (time.time() - start)))


# import asyncio
# from multiprocessing import Queue
# from threading import Thread
# import aiohttp
# from base import ExponentialBackOff
# from extensions.proxy import random_proxy
# class FinanceWorker2(Process):
#     def __init__(self, port=6009, send_every=60):
#         super(FinanceWorker2, self).__init__()
#         self.proxy_params = {'minDownloadSpeed': '150', 'protocol': 'http',
#                              'allowsHttps': 1, 'allowsUserAgentHeader': 1, 'maxConnectTime': '1'}
#         self.port = port
#         self.result_queue = Queue()
#         self.send_every = send_every
#         self.fake_users = UserAgent()
#         self.session = None
#         self.companies = sorted(Producer.load_query_dictionary('query_topics.txt').keys())
#
#     @staticmethod
#     def extract_stock(html_text):
#         """Scrapes stock price from html_text from google finance"""
#         stock_price = 0.0
#         tmp = re.search(r'id="ref_(.*?)">(.*?)<', html_text)
#         if tmp:
#             stock_price = eval(tmp.group(2).replace(',', ''))
#         return stock_price
#
#     async def fetch_stock(self, query):
#         """Query recent stock price for query using provided user agent and proxy"""
#         search_url = "http://www.google.com/finance?&q=" + query
#         retry_counter = ExponentialBackOff(1)
#         stock_price, proxy = 0.0, None
#         await asyncio.sleep(0)
#         async for _ in retry_counter:
#             try:
#                 if proxy is None:
#                     proxy = await random_proxy(**self.proxy_params)
#                 async with self.session.get(search_url, headers={'user-agent': self.fake_users.random}, proxy=proxy) as response:
#                     resp = await response.text()
#                 stock_price = self.extract_stock(resp)
#                 if response.status == 200:
#                     break
#             except aiohttp.client_exceptions.ClientError:
#                 self.session.close()
#                 self.session = aiohttp.ClientSession()
#             except (aiohttp.client_exceptions.TimeoutError, aiohttp.client_exceptions.ServerDisconnectedError, KeyError):
#                 proxy = None
#             except UnicodeDecodeError:
#                 proxy = None
#             except Exception as e:
#                 print('Finance', repr(e))
#                 proxy = None
#                 self.session.close()
#                 self.session = aiohttp.ClientSession()
#         self.result_queue.put(Item(content=stock_price, topic=query, source='stock'))
#
#     def send_to_outgoing(self):
#         outgoing = connection.Listener(('localhost', self.port), authkey=b'veryscrape').accept()
#         while True:
#             outgoing.send(self.result_queue.get())
#
#     async def stream(self):
#         self.session = aiohttp.ClientSession()
#         while True:
#             await asyncio.gather(*[self.fetch_stock(q) for q in self.companies])
#
#     def run(self):
#         Thread(target=self.send_to_outgoing).start()
#         policy = asyncio.get_event_loop_policy()
#         policy.set_event_loop(policy.new_event_loop())
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(self.stream())
#         self.session.close()
