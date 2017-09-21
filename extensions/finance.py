# Class to stream stock price data from Google finance
import asyncio
import re
import time

import aiohttp
from fake_useragent import UserAgent

from base import Producer, Item


def extract_stock(html_text):
    """Scrapes stock price from html_text from google finance"""
    stock_price = 0.
    tmp = re.search(r'id="ref_(.*?)">(.*?)<', html_text)
    if tmp:
        stock_price = eval(tmp.group(2).replace(',', ''))
    return stock_price


class FinanceWorker(Producer):
    def __init__(self, port=6009, send_every=60):
        super(FinanceWorker, self).__init__(port, False)
        self.send_every = send_every

    async def fetch_stocks(self, query):
        """Searches stock prices for all companies in query topics every minute"""
        query_url = "https://www.google.com/finance?&q={}"
        fake_users = UserAgent()
        sess = aiohttp.ClientSession()
        while self.running:
            start = time.time()
            # proxy = proxy_thread.random()
            async with sess.get(query_url.format(query), headers={'user-agent': fake_users.random}) as response:
                h = await response.text()
            price = extract_stock(h)
            self.result_queue.put(Item(price, query, 'stock'))
            await asyncio.sleep(max(0, self.send_every - (time.time() - start)))
        sess.close()

    def initialize_work(self):
        return [[self.fetch_stocks(q) for q in self.topics]]
