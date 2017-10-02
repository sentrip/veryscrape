import time
import unittest

from vs import SearchClient
from vs import synchronous, get_auth


class TestSearchClient(unittest.TestCase):
    test_server = 'http://posttestserver.com/'

    @synchronous
    async def setUp(self):
        self.client = SearchClient()

    @synchronous
    async def tearDown(self):
        await self.client.close()

    @synchronous
    async def test_update_proxy(self):
        assert self.client.proxy is None, "Instantiation failed"
        await self.client.update_proxy()
        assert self.client.proxy.startswith('http'), 'Incorrect proxy returned'

    @synchronous
    async def test_update_oauth2_token(self):
        auth = await get_auth('reddit')
        self.client.client, self.client.secret = auth[0]
        self.client.token_url = 'https://www.reddit.com/api/v1/access_token'
        await self.client.update_oauth2_token()
        assert self.client.token is not None, 'Token fetching was unsuccessful'

    @synchronous
    async def test_request_with_oauth2_token(self):
        auth = await get_auth('reddit')
        self.client.client, self.client.secret = auth[0]
        self.client.base_url = 'https://oauth.reddit.com/r/'
        self.client.token_url = 'https://www.reddit.com/api/v1/access_token'
        self.client.token_expiry = time.time() - 5
        async with await self.client.get('all/hot.json?raw_json=1', oauth=2) as resp:
            assert self.client.token is not None, 'Token fetching was unsuccessful'
            assert resp.status == 200, 'Request was not successfully executed'

    @synchronous
    async def test_request(self):
        async with self.client.request('GET', self.test_server) as resp:
            assert resp.status == 200, 'Request to test server was unsuccessful'

    @synchronous
    async def test_get(self):
        async with await self.client.get(self.test_server) as resp:
            assert resp.status == 200, 'Request to test server was unsuccessful'

    @synchronous
    async def test_post(self):
        async with await self.client.post(self.test_server + 'post.php', data={'a': 1}) as resp:
            assert resp.status == 200, 'Request to test server was unsuccessful'

    def test_instantiate_client(self):
        pass

    @synchronous
    async def test_build_arguments(self):
        url, params, aio_kwargs = await self.client.build_arguments('GET', '/test', False, None, {}, {})
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

    def test_extract_urls(self):
        urls = ['http://google.com/', 'http://myblogspot.blog.com/thing/article/1034.php']
        self.client.clean_urls(urls)
        assert len(urls) == 1, 'Urls were not successfully cleaned'

if __name__ == '__main__':
    unittest.main()
