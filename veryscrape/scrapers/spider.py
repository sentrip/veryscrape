import asyncio

from .google import extract_urls
from ..items import ItemGenerator
from ..session import fetch, Session
from ..scrape import Scraper


class SpiderItemGen(ItemGenerator):
    def __init__(self, *args, **kwargs):
        super(SpiderItemGen, self).__init__(*args, **kwargs)
        self.topic = '__classify__'


class Spider(Scraper):
    source = 'spider'
    scrape_every = 0
    item_gen = SpiderItemGen
    concurrent_requests = 200

    def __init__(self, *args, source_urls=(), proxy_pool=None):
        super(Spider, self).__init__(Session, *args, proxy_pool=proxy_pool)
        self.loop = asyncio.get_event_loop()
        self.source_urls = source_urls
        self.seen_urls = set()
        self.urls = set()

        self._futures = set()
        self._max_seen_urls = 1e7
        self._scrape_future = None

    async def close(self):
        if self._scrape_future is not None:
            self._scrape_future.cancel()
        await super(Spider, self).close()

    def scrape(self, query, topic='', **kwargs):
        if self._scrape_future is None:
            self._scrape_future = asyncio.ensure_future(self._scrape())
        return self._scrape_future

    async def _create_scrape_future(self, url):
        while len(self._futures) > self.concurrent_requests:
            await asyncio.sleep(1e-3)

        future = asyncio.ensure_future(fetch('GET', url, session=self.client))
        future.add_done_callback(self._fetch_callback)
        self._futures.add(future)

    def _fetch_callback(self, future):
        self._futures.remove(future)
        if not future.cancelled() and not future.exception():

            html = future.result()
            if html is not None:
                # Topic of data gathered by spider is classified later
                self.queues['__classify__'].put_nowait(html)

                for url in extract_urls(html):
                    if url not in self.seen_urls:
                        self.urls.add(url)
                        self.seen_urls.add(url)

    async def _scrape(self):
        self.urls.update(set(self.source_urls))
        while self._futures or self.urls:
            await asyncio.sleep(1e-2)
            for url in self.urls.copy():
                await self._create_scrape_future(url)
                self.urls.remove(url)
