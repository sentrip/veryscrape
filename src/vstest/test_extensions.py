import asyncio
import unittest

from veryscrape.extensions import BaseScraper, Twingly, Twitter, Reddit, Google, Finance
from vstest import synchronous


class TestExtensions(unittest.TestCase):

    @synchronous
    async def setUp(self):
        self.b = BaseScraper()
        self.q = asyncio.Queue()

    @synchronous
    async def tearDown(self):
        while not self.q.empty():
            _ = self.q.get_nowait()

    def test_dummy(self):
        pass

    @synchronous
    async def test_twingly(self):
        a = await self.b.get_api_data('twingly')
        twingly = Twingly(a)
        await twingly.scrape('whatsapp', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_reddit(self):
        a = await self.b.get_api_data('reddit')
        reddit = Reddit(a[0])
        await reddit.scrape('whatsapp', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_twitter_no_proxy(self):
        a = await self.b.get_api_data('twitter')
        twitter = Twitter(a[0])
        await twitter.scrape('whatsapp', 'FB', self.q, stream_for=5)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_google_no_proxy(self):
        google = Google()
        await google.scrape('whatsapp', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from google!'

    @synchronous
    async def test_finance_no_proxy(self):
        finance = Finance()
        await finance.scrape('FB', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from finance!'
