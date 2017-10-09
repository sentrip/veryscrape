import asyncio
import json
import re
import time
from collections import namedtuple
from functools import partial
from hashlib import sha1, md5
from random import SystemRandom
from urllib.parse import urljoin

import aiohttp
from aioauth_client import HmacSha1Signature
from fake_useragent import UserAgent
from retrying import retry

random = SystemRandom().random
Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


class BaseScraper:
    # Proxy & api data servers
    proxy_url = 'http://192.168.0.100:9999'
    api_url = 'http://192.168.0.100:1111'
    # Base request characteristics
    base_url = 'http://example.com'
    user_agent = None
    persist_user_agent = True
    # OAuth 1 & 2
    client, secret, token, token_secret = None, None, None, None
    signature = HmacSha1Signature()
    # OAuth 2
    token_url = None
    token_expiry = 0
    # Rate limits
    rate_limit = 0  # Requests per minute
    request_count = 0
    last_removed = time.clock()
    proxy_params, proxy = None, None
    # Unique item filtering
    seen = set()

    def setup(self, query):
        """Overwrite with setup for scrape, return functools.partial of build_requests with appropriate arguments"""
        return lambda: partial(asyncio.sleep, 1)

    def handle_response(self, resp, topic, queue, **kwargs):
        """Overwrite with response handling for scrape - response needs to be awaited (e.g. await resp.text())"""
        pass

    async def build_request(self, method, url, *, oauth=0, params=None, use_proxy=False, **aio_kwargs):
        """Waits for rate limit if defined, and updates request parameters with necessary oauth data"""
        headers = {'user-agent': self._user_agent}
        while self.rate_limit_exceeded:
            self.update_rate_limit()
            await asyncio.sleep(0)

        if not url.startswith('http'):
            url = urljoin(self.base_url, url)

        if oauth == 1:
            params.update(self.oauth1_parameters)
            params['oauth_signature'] = self.signature.sign(self.secret, method, url, self.token_secret, **params)

        elif oauth == 2:
            auth = await self.oauth2_parameters
            headers.update(auth)

        if use_proxy:
            await self.update_proxy()
            aio_kwargs.update({'proxy': self.proxy})

        aio_kwargs.update({'headers': headers})
        return method, url, params, aio_kwargs

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
            res = await self.get_from_local_server(self.proxy_url, self.proxy_params)
            if res is not None:
                self.proxy = res

    async def get_api_data(self, q):
        """Queries local api data server for data type specified by q"""
        res = await self.get_from_local_server(self.api_url, {'q': q})
        return eval(res)

    @staticmethod
    def clean_urls(urls):
        """Generator for removing useless or uninteresting urls from an iterable of urls"""
        bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        for u in urls:
            is_root_url = any(u.endswith(j) for j in bad_domains)
            is_not_relevant = any(j in u for j in false_urls)
            if u.startswith('http') and not (is_root_url or is_not_relevant):
                yield u

    @staticmethod
    async def get_from_local_server(url, params):
        """Gets from the local server specified in client setup"""
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, params=params) as resp:
                res = await resp.text()
        if not res.startswith('Incorrect'):
            return res

    def filter(self, item):
        """Returns False if the item has been seen before, True if not"""
        hsh = md5(item.encode()).hexdigest()
        if hsh not in self.seen:
            self.seen.add(hsh)
            if len(self.seen) >= 50000:
                self.seen.pop()
            return True
        return False

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_jitter_max=500)
    async def scrape(self, query, topic, queue, use_proxy=False, setup=None, resp_handler=None, **rh_kwargs):
        """Scrapes resources provided in setup method and handles response with handle_response method"""
        if setup is None:
            setup = self.setup
        if resp_handler is None:
            resp_handler = self.handle_response
        args = [query] + ([use_proxy] if use_proxy else [])
        setup = setup(*args)
        method, url, params, kwargs = await setup()
        async with aiohttp.ClientSession() as sess:
            async with sess.request(method, url, params=params, **kwargs) as raw:
                await resp_handler(raw, topic, queue, **rh_kwargs)


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
            chunk = await self.raw.content.read(self.chunk_size)
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
