import asyncio
import json
import re
import time
from hashlib import sha1
from random import SystemRandom
from urllib.parse import urljoin

import aiohttp
from aioauth_client import HmacSha1Signature
from fake_useragent import UserAgent

random = SystemRandom().random


async def get_auth(auth_type):
    """Requests api server for corresponding authentication information"""
    api_url = 'http://192.168.0.100:1111'
    async with aiohttp.ClientSession() as sess:
        async with sess.get(api_url, params={'type': auth_type}) as response:
            resp = await response.text()
    try:
        return json.loads(resp)['auth']
    except (KeyError, TypeError, json.JSONDecodeError):
        return []


class RequestBuilder:
    base_url = 'http://example.com'
    user_agent = None
    persist_user_agent = True
    # OAuth 1 & 2
    client = None
    secret = None
    token = None
    token_secret = None
    signature = HmacSha1Signature()
    # OAuth 2
    token_url = None
    token_expiry = 0
    # Rate limits
    rate_limit = 0  # Requests per minute
    request_count = 0
    last_removed = time.clock()
    # Proxy server
    proxy_url = 'http://192.168.0.100:9999'
    proxy_params = None
    proxy = None

    async def build_request_arguments(self, method, url, *, oauth=False, params=None, use_proxy=False, **aio_kwargs):
        """Waits for rate limit if defined, and updates request parameters with necessary oauth data"""
        while self.rate_limit_exceeded:
            self.update_rate_limit()
            await asyncio.sleep(0)

        if not url.startswith('http'):
            urljoin(self.base_url, url)

        if oauth == 1:
            params.update(self.oauth1_parameters)
            params['oauth_signature'] = self.signature.sign(self.secret, method, url, self.token_secret, **params)

        elif oauth == 2:
            if self.oauth2_token_expired:
                aio_kwargs.update(await self.oauth2_parameters)

        if use_proxy:
            await self.update_proxy()
            aio_kwargs.update(proxy=self.proxy)

        aio_kwargs.update(headers={'user-agent': self._user_agent})
        return url, params, aio_kwargs

    def update_rate_limit(self):
        """Decrements request count by number of allowed requests since last update"""
        now = time.clock()
        since = now - self.last_removed
        if since >= 1:
            diff = int(since / 60 * self.rate_limit)
            self.request_count = max(0, self.request_count - diff)
            self.last_removed = now if diff else self.last_removed

    @property
    def _user_agent(self):
        """Creates new random user agent, sets self.user agent and returns, otherwise returns current user agent"""
        if self.user_agent is None or not self.persist_user_agent:
            self.user_agent = UserAgent().random
            return self.user_agent
        else:
            return self.user_agent

    @property
    def rate_limit_exceeded(self):
        """Returns true if rate limit is defined and exceeded"""
        return self.rate_limit and self.request_count > self.rate_limit

    @property
    def oauth2_token_expired(self):
        """Returns true if current oauth2 token needs to be refreshed"""
        return (time.time() >= self.token_expiry and self.token_expiry) or self.token is None

    @property
    def oauth1_parameters(self):
        """Returns dictionary of oauth1 parameters required for oauth1 signed http request"""
        return {'oauth_consumer_key': self.client,
                'oauth_token': self.token,
                'oauth_signature_method': self.signature.name,
                'oauth_nonce': sha1(str(random()).encode('ascii')).hexdigest(),
                'oauth_timestamp': str(int(time.time())),
                'oauth_version': '1.0'}

    @property
    async def oauth2_parameters(self):
        """Returns dictionary of oauth2 parameters required for oauth1 signed http request"""
        if self.oauth2_token_expired:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(self.token_url, data={'grant_type': 'client_credentials'},
                                     auth=aiohttp.BasicAuth(self.client, self.secret)) as resp:
                    auth = await resp.json()
            self.token = auth['access_token']
            try:
                self.token_expiry = int(time.time()) + int(auth['expires_in'])
            except KeyError:
                self.token_expiry = 0
        return {'Authorization': 'bearer ' + self.token}

    async def update_proxy(self):
        """Updates current proxy with a proxy that has the characteristics specified in params"""
        if self.proxy_params is not None:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(self.proxy_url, params=self.proxy_params) as resp:
                    res = await resp.text()
            if not res.startswith('Incorrect'):
                self.proxy = res


class ReadBuffer:
    """Asynchronous stream data read buffer"""
    def __init__(self, stream, chunk_size=1024):
        enc_search = re.search('charset=(?P<enc>\S*)', stream.headers.get('content-type', ''))
        self.encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'
        self.buffer = b''
        self.chunk_size = chunk_size
        self.raw = stream

    def __aiter__(self):
        return self

    async def next_chunk(self):
        """Yields next chunk of stream as a byte string"""
        chunk = b''
        try:
            chunk = await self.raw.read(self.chunk_size)
            self.buffer += chunk
        except (aiohttp.Timeout, aiohttp.ClientPayloadError):
            self.raw.close()
        finally:
            if not chunk:
                raise StopAsyncIteration

    async def next_item(self):
        """Yields next `item` in stream (byte sting seperated by \n)"""
        item = b''
        while not item:
            index = self.buffer.find(b'\n')
            if index > -1:
                item, self.buffer = self.buffer[:index], self.buffer[index + 1:]
                if item == b'\r':
                    item = b''
            else:
                await self.next_chunk()
        return item

    async def __anext__(self):
        """Yields next item from data stream - use with async for loop"""
        item = await self.next_item()
        status = json.loads(item.decode(self.encoding))
        if 'limit' in status:
            await asyncio.sleep((float(status['limit']['track']) + float(
                status['limit']['timestamp_ms'])) / 1000 - time.time())
            status = await self.__anext__()
        return status
