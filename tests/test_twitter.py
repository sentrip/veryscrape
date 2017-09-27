import asyncio
import json
import unittest
from collections import namedtuple
from functools import wraps
from io import BytesIO

import veryscrape
from veryscrape.extensions.twitter import Twitter, ReadBuffer


def run_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        l = asyncio.get_event_loop()
        return l.run_until_complete(f(*args, **kwargs))
    return wrapper


class TestTwitter(unittest.TestCase):
    def setUp(self):
        self.auth = veryscrape.Producer.load_authentications('twitter.txt')

    def test_readbuffer_small_stream(self):
        tweets = [{'status': 'This tweet'}, {'status': 'This tweet'}]
        mock_stream_content = bytes('{}\r\n\r{}\r\n'.format(*list(map(json.dumps, tweets))), encoding='utf-8')
        mock_stream = BytesIO(mock_stream_content)
        mock_stream.headers = {}

        @run_async
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

        @run_async
        async def read():
            buf = ReadBuffer(mock_stream)
            async for item in buf:
                assert item == 'This item', 'Did not return same item!'
        read()

    def test_twitter_acquire_stream(self):
        params = {'language': 'en', 'track': 'apple'}

        @run_async
        async def read():
            twitter = Twitter(self.auth['FB'])
            raw = await twitter.request('POST', 'statuses/filter.json', params=params, oauth=1, stream=True)
            await twitter.close()
            assert raw.status == 200, 'Request failed, returned error code {}'.format(raw.status)
        read()

    def test_twitter_filter_stream_no_proxy(self):
        @run_async
        async def read():
            twitter = Twitter(self.auth['AAPL'])
            await twitter.filter_stream('apple', 'AAPL', duration=1, use_proxy=False)
            await twitter.close()
        read()

    def test_twitter_filter_stream_proxy(self):
        @run_async
        async def read():
            twitter = Twitter(self.auth['AAPL'])
            await twitter.filter_stream('apple', 'AAPL', duration=1, use_proxy=True)
            await twitter.close()
        read()

if __name__ == '__main__':
    unittest.main()
