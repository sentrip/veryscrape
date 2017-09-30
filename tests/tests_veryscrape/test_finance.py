import asyncio
import unittest

from veryscrape import load_query_dictionary, synchronous
from veryscrape.extensions.finance import Finance


class TestFinance(unittest.TestCase):
    topics = load_query_dictionary('query_topics')
    topic = next(iter(list(topics.keys())))

    @synchronous
    async def setUp(self):
        self.url_queue = asyncio.Queue()
        self.client = Finance(self.url_queue)

    @synchronous
    async def tearDown(self):
        await self.client.close()
        while not self.url_queue.empty():
            _ = self.url_queue.get_nowait()

    @synchronous
    async def test_finance_single_request_no_proxy(self):
        resp = await self.client.get(self.client.finance_search_path, params={'q': self.topic}, stream=True)
        assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
        resp.close()

    @synchronous
    async def test_finance_stream_no_proxy(self):
        await self.client.finance_stream(self.topic, duration=0.1, send_every=0)
        assert self.url_queue.get_nowait(), "Url queue empty!"

    # @synchronous
    # async def test_finance_single_request_proxy(self):
    #     resp = await self.client.get(self.client.finance_search_path, params={'q': self.topic},
    #                                  stream=True, use_proxy={'speed': 50, 'https': 1})
    #     assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
    #     resp.close()
    #
    # @synchronous
    # async def test_finance_stream_proxy(self):
    #     await self.client.finance_stream(self.topic, duration=0.1, send_every=0, use_proxy=True)
    #     assert self.url_queue.get_nowait(), "Url queue empty!"

if __name__ == '__main__':
    unittest.main()
