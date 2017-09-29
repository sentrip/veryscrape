import unittest
from multiprocessing import Queue

from veryscrape import load_query_dictionary, synchronous
from veryscrape.extensions.google import Google


class TestGoogle(unittest.TestCase):
    topics = load_query_dictionary('query_topics')
    topic = next(iter(list(topics.keys())))
    q = 'tesla'
    url_queue = Queue()

    @synchronous
    async def setUp(self):
        self.client = Google(self.url_queue)

    @synchronous
    async def tearDown(self):
        await self.client.close()
        while not self.url_queue.empty():
            _ = self.url_queue.get_nowait()

    @synchronous
    async def test_google_single_request_no_proxy(self):
        resp = await self.client.get(self.client.article_search_path.format(self.q, self.q), stream=True)
        assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
        resp.close()

    @synchronous
    async def test_google_stream_no_proxy(self):
        await self.client.article_stream(self.q, self.topic, duration=0.1)
        assert self.url_queue.get_nowait(), "Url queue empty!"

    # @synchronous
    # async def test_google_single_request_proxy(self):
    #     resp = await self.client.get(self.client.article_search_path.format(self.q, self.q),
    #                                  stream=True, use_proxy={'speed': 50, 'https': 1})
    #     assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
    #     resp.close()
    #
    # @synchronous
    # async def test_google_stream_proxy(self):
    #     await self.client.article_stream(self.q, self.topic, duration=0.1, use_proxy=True)
    #     assert self.url_queue.get_nowait(), "Url queue empty!"

if __name__ == '__main__':
    unittest.main()
