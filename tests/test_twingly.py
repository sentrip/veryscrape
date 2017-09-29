# TOdo write twingly tests
# todo add text for url extraction in client tests

import unittest
from multiprocessing import Queue

from veryscrape import Producer, synchronous, get_auth
from veryscrape.extensions.twingly import Twingly


@synchronous
async def f():
    return await get_auth('twingly')


class TestTwingly(unittest.TestCase):
    topics = Producer.load_query_dictionary('query_topics')
    topic = next(iter(list(topics.keys())))
    q = 'tesla'
    auth = f()[1][0]
    url_queue = Queue()

    @synchronous
    async def setUp(self):
        self.client = Twingly(self.auth, self.url_queue)

    @synchronous
    async def tearDown(self):
        await self.client.close()

    @synchronous
    async def test_twingly_single_request(self):
        params = {'apiKey': self.client.client, 'q': self.client.build_query(self.q)}
        resp = await self.client.get(self.client.blog_search_path, params=params, stream=True)
        assert resp.status != 401, 'Twingly search unauthorized'
        assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
        resp.close()

    @synchronous
    async def test_twingly_stream(self):
        await self.client.blog_stream(self.q, self.topic, duration=1)
        assert self.url_queue.get_nowait(), "Url queue empty!"

if __name__ == '__main__':
    unittest.main()
