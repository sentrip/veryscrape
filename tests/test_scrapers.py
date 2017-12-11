import asyncio
import logging
import unittest

from tests import synchronous, LOG_LEVEL
from veryscrape.scrapers import Twitter, Reddit, Google

log = logging.getLogger(__name__)
logging.basicConfig(level=LOG_LEVEL)


class TestExtensions(unittest.TestCase):
    twitter_auth = 'syMCJbFpu5yWMWMFoDKsmCMmS|ZxGFgNXkwRhnMV1VZ532mKwOkKnEMUJYS5LwvhmiBAVCIDb7VD|428778312-W98LprTMPP60CtjdwGtthQchpxUSL2nKw6a2KE1C|n2YXS6zmFSCeo4OenksbytXvzokRNVplN68zNeZ7Djams'.split('|')
    reddit_auth = 'M6GNWAhYiveMow|Z7gBb23v77BoNGc_BEJ8MYCStWM'.split('|')
    twingly_auth = ''

    def setUp(self):
        self.q = asyncio.Queue()

    @synchronous
    async def tearDown(self):
        while not self.q.empty():
            _ = self.q.get_nowait()

    @synchronous
    async def test_reddit(self):
        reddit = Reddit(self.reddit_auth, output_queue=self.q)
        await reddit.scrape('Bitcoin', 'FB', 1)
        assert self.q.qsize() > 0, 'No items retrieved from reddit!'

    @synchronous
    async def test_twitter_no_proxy(self):
        twitter = Twitter(self.twitter_auth, output_queue=self.q)
        await twitter.scrape('whatsapp', 'FB', 1)
        assert self.q.qsize() > 0, 'No items retrieved from twitter!'

    @synchronous
    async def test_google_no_proxy(self):
        google = Google(output_queue=self.q)
        await google.scrape('whatsapp', 'FB')
        assert self.q.qsize() > 0, 'No items retrieved from google!'

    # @synchronous
    # async def test_twingly(self):
    #     twingly = Twingly(self.twingly_auth)
    #     await twingly.scrape('whatsapp', 'FB', self.q)
    #     assert self.q.qsize() > 0, 'No items retrieved from twingly!'
