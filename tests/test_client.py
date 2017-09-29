import time
import unittest

from veryscrape import synchronous
from veryscrape.client import SearchClient


class TestSearchClient(unittest.TestCase):

    @synchronous
    async def setUp(self):
        self.client = SearchClient()

    @synchronous
    async def tearDown(self):
        await self.client.close()

    @synchronous
    async def test_update_proxy(self):
        pass

    @synchronous
    async def test_send_item(self):
        pass

    @synchronous
    async def test_update_oauth2_token(self):
        pass

    @synchronous
    async def test_request(self):
        pass

    @synchronous
    async def test_get(self):
        pass

    @synchronous
    async def test_post(self):
        pass

    def test_instantiate_client(self):
        pass

    def test_build_arguments(self):
        url, params, aio_kwargs = self.client.build_arguments('GET', '/test', False, None, {}, {})
        assert url.startswith('http'), 'Invalid url returned'
        assert isinstance(params, dict), 'Invalid parameters returned'
        assert isinstance(aio_kwargs, dict), 'Invalid aio-http kwargs returned'

    def test_update_rate_limit_with_limit(self):
        self.client.rate_limit = 30
        self.client.request_count = 60
        self.client.rate_limit_clock = time.time() - 60
        assert self.client.rate_limit_exceeded, 'Rate limit exceeded property incorrectly fired'
        self.client.update_rate_limit()
        assert self.client.request_count == 30, 'Request count incorrectly decremented'

    def test_update_rate_limit_without_limit(self):
        self.client.rate_limit = 0
        self.client.request_count = 60
        self.client.rate_limit_clock = time.time() - 60
        assert not self.client.rate_limit_exceeded, 'Rate limit exceeded property incorrectly fired'
        self.client.update_rate_limit()
        assert self.client.request_count == 60, 'Request count was decremented when it shouldn\'t have been'

if __name__ == '__main__':
    unittest.main()
