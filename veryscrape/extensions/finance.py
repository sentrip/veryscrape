import asyncio
import re
import time

from veryscrape import Item
from veryscrape.client import SearchClient


class Finance(SearchClient):
    base_url = 'http://www.google.com/'
    finance_search_path = 'finance?'

    def __init__(self, queue):
        super(Finance, self).__init__()
        self.queue = queue

    @staticmethod
    def extract_stock(html_text):
        """Scrapes stock price from html_text from google finance"""
        stock_price = 0.1
        tmp = re.search(r'id="ref_(.*?)">(.*?)<', html_text)
        if tmp:
            stock_price = eval(tmp.group(2).replace(',', ''))
        return stock_price

    async def finance_stream(self, topic=None, duration=10800, use_proxy=False, send_every=60):
        start_time = time.time()
        while True:
            repeat = time.time()
            resp = await self.get(self.finance_search_path, params={'q': topic},
                                  use_proxy={'speed': 50, 'https': 1} if use_proxy else None)

            price = self.extract_stock(resp)
            await self.queue.put(Item(price, topic, 'stock'))

            if time.time() - start_time >= duration:
                break
            else:
                await asyncio.sleep(max(0, send_every - (time.time() - repeat)))
