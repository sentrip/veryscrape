import json
import time
import unittest
from collections import namedtuple
from io import BytesIO

from tests import synchronous
from veryscrape.api import BaseScraper, ReadBuffer


class TestBaseScraper(unittest.TestCase):

    @synchronous
    async def setUp(self):
        self.builder = BaseScraper()
        self.builder.token_url = 'https://www.reddit.com/api/v1/access_token'

    @synchronous
    async def test_update_rate_limit_with_no_limit(self):
        """Checks if updating rate limit with no rate limit set does not update request count"""
        self.builder.last_removed = -60
        self.builder.request_count = 100
        self.builder.update_rate_limit()
        assert self.builder.request_count == 100, \
            "Request count was changed with no rate limit, {}, {}".format(self.builder.request_count, 100)

    @synchronous
    async def test_update_rate_limit_with_limit(self):
        """Checks if updating rate limit with no rate limit set does not update request count"""
        self.builder.rate_limit = 43
        self.builder.last_removed = -60
        self.builder.request_count = 100
        self.builder.update_rate_limit()
        assert self.builder.request_count <= 100 - 43, \
            "Request count was not decremented correctly, {}, {}".format(self.builder.request_count, 100 - 43)

    # @synchronous
    # async def test_get_api_data(self):
    #     b = BaseScraper()
    #     types = ['twingly', 'reddit', 'twitter', 'proxy', 'topics', 'subreddits']
    #     auths = await asyncio.gather(*[b.get_api_data(t) for t in types])
    #     for auth in auths:
    #         assert any(isinstance(auth, i) for i in [list, str, dict]), \
    #             'Incorrect auth data type returned!, {}'.format(type(auth))
    #         assert len(auth) >= 1, 'No authentications returned, {}'.format(auth)

    @synchronous
    async def test_update_proxy_no_proxy_required(self):
        """Tests if update proxies does nothing if a proxy already exists"""
        self.builder.proxy = 'true'
        await self.builder.update_proxy()
        assert self.builder.proxy == 'true', \
            'Proxy was updated when it should not have been, {}'.format(self.builder.proxy)

    @synchronous
    async def test_update_proxy_proxy_required_no_params(self):
        """Tests if update proxy correctly updates proxy when required with no proxy parameters"""
        self.builder.proxy_params = {}
        await self.builder.update_proxy()
        assert self.builder.proxy.startswith('http'), \
            'Proxy was updated when it should not have been, {}'.format(self.builder.proxy)

    @synchronous
    async def test_update_proxy_proxy_required_params(self):
        """Tests if update proxy correctly updates proxy when required with proxy parameters"""
        self.builder.proxy_params = {'speed': 1}
        await self.builder.update_proxy()
        assert self.builder.proxy.startswith('http'), \
            'Proxy was not updated when it should not have been, {}'.format(self.builder.proxy)

    @synchronous
    async def test_update_oauth2_no_token(self):
        """Test oauth2 token retrieval when new token is required"""
        self.builder.client, self.builder.secret = 'VbKYA5v77UPooA', 'OZan8kt5EluEZ0pXpMbmtLoPTgk'
        headers = await self.builder.oauth2_parameters
        assert headers == {'Authorization': 'bearer ' + self.builder.token}, \
            'Incorrect headers returned'

    @synchronous
    async def test_update_oauth2_token_exists_not_expired(self):
        """Test oauth2 token retrieval if current token is unexpired"""
        self.builder.token = 'hello'
        self.builder.token_expiry = time.time() + 7200
        headers = await self.builder.oauth2_parameters
        assert headers == {'Authorization': 'bearer ' + 'hello'}, \
            'Incorrect headers returned'

    @synchronous
    async def test_update_oauth2_token_exists_expired(self):
        """Test oauth2 token retrieval when current token has expired"""
        self.builder.token = 'hello'
        self.builder.client, self.builder.secret = 'VbKYA5v77UPooA', 'OZan8kt5EluEZ0pXpMbmtLoPTgk'
        self.builder.token_expiry = time.time() - 10
        headers = await self.builder.oauth2_parameters
        assert self.builder.token != 'hello', 'Token was not updated when it should have been'
        assert headers == {'Authorization': 'bearer ' + self.builder.token}, \
            'Incorrect headers returned'

    @synchronous
    async def test_clean_urls(self):
        """Tests whether the url sanitizing generator correctly removes bad urls"""
        urls = ['https://google.com', '/path/to/some.shit', 'http://youtube.com']
        clean = ['http://somespecialthing.io/articlestuff', 'http://quenetasd.io/articlestuff']
        urls += clean
        b = BaseScraper()
        clean_urls = [i for i in b.clean_urls(urls)]
        assert clean == clean_urls, 'Did not correctly remove junk urls!'

    @synchronous
    async def test_filter(self):
        """Tests whether the unique filter is correctly able to remove all duplicates"""
        alls = ['abc', 'adc', 'gbc', 'abc']
        b = BaseScraper()
        uniques = [i for i in alls if b.filter(i)]
        assert len(uniques) == 3, 'Filter did not correctly remove the duplicate from the list!'


class TestReadBuffer(unittest.TestCase):
    @synchronous
    async def test_read_buffer_no_data_in_stream(self):
        ms = namedtuple('MockStream', ['content', 'headers'])
        mock_stream = ms(BytesIO(bytes('', encoding='utf-8')), {})
        buf = ReadBuffer(mock_stream)
        c = 0
        async for _ in buf:
            c += 1
        assert c == 0, "Items were yielded from an empty buffer!"

    @synchronous
    async def test_read_buffer_data_in_stream(self):
        tweets = [{'status': 'This tweet'}] * 10000
        ms = namedtuple('MockStream', ['content', 'headers'])
        s = '\r\n\r'.join(['{}']*10000) + '\r\n'
        mock_stream_content = bytes(s.format(*list(map(json.dumps, tweets))), encoding='utf-8')
        mock_stream = ms(BytesIO(mock_stream_content), {})
        buf = ReadBuffer(mock_stream)
        async for item in buf:
            assert item == 'This item', 'Did not return same item!'
