import asyncio
import json
import unittest
from collections import namedtuple
from io import BytesIO

from veryscrape import synchronous, get_auth, load_query_dictionary
from veryscrape.extensions.twitter import Twitter, ReadBuffer


class TestTwitter(unittest.TestCase):

    @synchronous
    async def setUp(self):
        self.topics = load_query_dictionary('query_topics')
        self.auth = {k: a for k, a in zip(sorted(self.topics.keys()), await get_auth('twitter'))}
        self.queue = asyncio.Queue()
        self.twitter = Twitter(self.auth['FB'], self.queue)

    @synchronous
    async def tearDown(self):
        await self.twitter.close()
        while not self.queue.empty():
            _ = self.queue.get_nowait()

    def test_readbuffer_small_stream(self):
        tweets = [{'status': 'This tweet'}, {'status': 'This tweet'}]
        mock_stream_content = bytes('{}\r\n\r{}\r\n'.format(*list(map(json.dumps, tweets))), encoding='utf-8')
        mock_stream = BytesIO(mock_stream_content)
        mock_stream.headers = {}

        @synchronous
        async def read():
            buf = ReadBuffer(mock_stream)
            async for item in buf:
                assert item == 'This item', 'Did not return same item!'
        read()

    def test_readbuffer_large_stream(self):
        tweets = [{'status': 'This tweet'}] * 10000
        ms = namedtuple('MockStream', ['content', 'headers'])
        s = '\r\n\r'.join(['{}']*10000) + '\r\n'
        mock_stream_content = bytes(s.format(*list(map(json.dumps, tweets))), encoding='utf-8')
        mock_stream = ms(BytesIO(mock_stream_content), {})

        @synchronous
        async def read():
            buf = ReadBuffer(mock_stream)
            async for item in buf:
                assert item == 'This item', 'Did not return same item!'
        read()

    def test_twitter_acquire_stream(self):
        params = {'language': 'en', 'track': 'apple'}

        @synchronous
        async def read():
            raw = await self.twitter.request('POST', 'statuses/filter.json', params=params, oauth=1, stream=True)
            assert raw.status == 200, 'Request failed, returned error code {}'.format(raw.status)
        read()

    def test_twitter_filter_stream_no_proxy(self):
        @synchronous
        async def read():
            await self.twitter.filter_stream('apple', 'AAPL', duration=1, use_proxy=False)
        read()

    # def test_twitter_filter_stream_proxy(self):
    #     @synchronous
    #     async def read():
    #         twitter = Twitter(self.auth['AAPL'])
    #         await twitter.filter_stream('apple', 'AAPL', duration=1, use_proxy=True)
    #         await twitter.close()
    #     read()

if __name__ == '__main__':
    unittest.main()