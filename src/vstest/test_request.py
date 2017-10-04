import asyncio
import json
import time
import unittest
from collections import namedtuple
from io import BytesIO

from veryscrape.request import get_auth, RequestBuilder, ReadBuffer
from vstest import synchronous


class TestRequestBasicFunctions(unittest.TestCase):

    @synchronous
    async def test_get_auth(self):
        types = ['twingly', 'reddit', 'twitter']
        auths = await asyncio.gather(*[get_auth(t) for t in types])
        for auth in auths:
            assert isinstance(auth, list), 'Incorrect auth data type returned!, {}'.format(type(auth))
            assert len(auth) >= 1, 'No authentications returned, {}'.format(auth)

    @synchronous
    async def test_fetch(self):
        """Tests the simple http fetch method"""
        # todo implement test
        pass

    @synchronous
    async def test_download(self):
        """Tests continuous download coroutine to ensure urls are being downloaded"""
        # todo implement test
        pass


class TestRequestBuilder(unittest.TestCase):

    @synchronous
    async def setUp(self):
        self.builder = RequestBuilder()
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
        # todo implement test
        pass

    @synchronous
    async def test_filter(self):
        """Tests whether the unique filter is correcly able to remove all duplicates"""
        # todo implement test
        pass

    @synchronous
    async def test_scrape(self):
        """Tests whether the scrape function correctly handles an http request and response"""
        # todo implement test
        pass


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
