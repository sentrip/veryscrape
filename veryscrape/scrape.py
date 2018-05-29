from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial
import asyncio
import logging
import time

from .items import ItemGenerator
from .session import fetch

log = logging.getLogger(__name__)


class Scraper(ABC):
    source = ''
    scrape_every = 5 * 60
    item_gen = ItemGenerator

    def __init__(self, client, *args, proxy_pool=None, **kwargs):
        self.client = client(*args, proxy_pool=proxy_pool, **kwargs)
        self.queues = defaultdict(asyncio.Queue)
        self._stream = None

    @abstractmethod
    async def scrape(self, query, topic='', **kwargs):
        raise NotImplementedError  # pragma: nocover

    async def scrape_continuously(self, query, topic='', **kwargs):
        while True:
            start = time.time()
            await self.scrape(query, topic=topic, **kwargs)
            await asyncio.sleep(
                max(0., self.scrape_every - (time.time() - start))
            )

    def stream(self, query, topic='', **kwargs):
        self._stream = asyncio.ensure_future(
            self.scrape_continuously(query, topic=topic, **kwargs)
        )
        log.debug('Scraping %s: TOPIC=%s,  QUERY=%s',
                  self.source, topic, query)
        return self.item_gen(self.queues[topic],
                             topic=topic, source=self.source)

    async def close(self):
        self._stream.cancel()
        await self.client.close()


class HTMLScraper(Scraper):
    bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
    false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}

    @abstractmethod
    def query_string(self, query):
        raise NotImplementedError  # pragma: nocover

    @abstractmethod
    def extract_urls(self, text):
        raise NotImplementedError  # pragma: nocover

    @staticmethod
    def clean_urls(urls):
        """
        Generator for removing useless or uninteresting urls
        from an iterable of urls
        """
        for u in urls:
            is_root_url = any(u.endswith(j) for j in HTMLScraper.bad_domains)
            is_not_relevant = any(j in u for j in HTMLScraper.false_urls)
            if u.startswith('http') and not (is_root_url or is_not_relevant):
                yield u
            else:
                log.debug('Removing unclean url: %s', u)

    async def scrape(self, query, topic='', **kwargs):
        url = self.query_string(query)
        _html = await fetch('GET', url, session=self.client, **kwargs)
        links, created_times = self.extract_urls(_html)
        links = list(links)

        futures = []
        for link in self.clean_urls(links):
            future = asyncio.ensure_future(fetch(
                'GET', link, session=self.client, **kwargs))
            cb = partial(self._put_future,
                         topic=topic,
                         created_at=created_times[links.index(link)])
            future.add_done_callback(cb)
            futures.append(future)
        await asyncio.gather(*futures)

    def _put_future(self, future, topic='', created_at=None):
        if not future.cancelled() and not future.exception():
            res = future.result()
            if res is not None:
                self.queues[topic].put_nowait((res, created_at))
