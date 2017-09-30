import asyncio
import time

import lxml.html as html

from veryscrape import Item
from veryscrape.client import SearchClient


class Google(SearchClient):
    base_url = 'https://news.google.com/news/search/section/q'
    article_search_path = '{}/{}?hl=en&ned=us'

    def __init__(self, queue):
        super(Google, self).__init__()
        self.queue = queue

    @staticmethod
    def extract_urls(resp):
        urls = []
        result = html.fromstring(resp)
        for e in result.xpath('//*[@href]'):
            if e.get('href') is not None:
                urls.append(e.get('href'))
        return urls

    async def article_stream(self, track=None, topic=None, duration=10800, use_proxy=False):
        start_time = time.time()
        while True:
            resp = await self.get(self.article_search_path.format(track, track),
                                  use_proxy={'speed': 50, 'https': 1} if use_proxy else None)

            urls = self.extract_urls(resp)
            for url in urls:
                await self.queue.put(Item(url, topic, 'article'))

            if time.time() - start_time >= duration:
                break
            else:
                await asyncio.sleep(900)
