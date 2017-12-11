import asyncio
import logging
import unittest

from tests import synchronous, LOG_LEVEL
from veryscrape.scrapers import Twingly, Twitter, Reddit, Google

log = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)


class TestExtensions(unittest.TestCase):
    twitter_auth = 'aU3F2YbHj6xWwh43LPhd3kCIh|SYR9t1L3o2nmk3AOaadt3qkqZBzyFO7a7pyl62gLzr284mxTPz|900128484709859328-Fd8LVAky16l7vamEYY0vNTfiOMsneT7|pE4iyAAbgvv3avLdcUO7mGjcBNC08nweHs4b9lbHuyaCD'.split('|')
    reddit_auth = 'M6GNWAhYiveMow|Z7gBb23v77BoNGc_BEJ8MYCStWM'.split('|')
    twingly_auth = ''

    def setUp(self):
        self.q = asyncio.Queue()

    @synchronous
    async def tearDown(self):
        while not self.q.empty():
            _ = self.q.get_nowait()

    @synchronous
    async def test_twingly(self):
        twingly = Twingly(self.twingly_auth)
        await twingly.scrape('whatsapp', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_reddit(self):
        reddit = Reddit(self.reddit_auth)
        await reddit.scrape('DrPepperOcean', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_twitter_no_proxy(self):
        twitter = Twitter(self.twitter_auth)
        await twitter.scrape('whatsapp', 'FB', self.q, stream_for=5)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_google_no_proxy(self):
        google = Google()
        await google.scrape('whatsapp', 'FB', self.q)
        assert self.q.qsize() > 0, 'No items retrieved from google!'

    @synchronous
    async def test_twitter_proxy(self):
        twitter = Twitter(self.twitter_auth)
        await twitter.scrape('whatsapp', 'FB', self.q, use_proxy=True, stream_for=5)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_google_proxy(self):
        google = Google()
        await google.scrape('whatsapp', 'FB', self.q, use_proxy=True)
        assert self.q.qsize() > 0, 'No items retrieved from google!'
