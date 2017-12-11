import asyncio
import logging
import re
import time
from abc import ABC, abstractmethod
from collections import namedtuple
from hashlib import sha1, md5
from random import SystemRandom
from urllib.parse import urljoin, quote

import aiohttp
from aioauth_client import HmacSha1Signature
from fake_useragent import UserAgent
from retrying import retry

log = logging.getLogger('veryscrape')
random = SystemRandom().random
Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


class BreakerError(BaseException):
    pass


class Breaker:
    def __init__(self, fail_max=5, reset_timeout=60):
        self.fail_max = fail_max
        self.reset_timeout = reset_timeout
        self.break_start = 0
        self.fails = 0

    def __call__(self, f):
        async def w(*args, **kwargs):
            if self.fails >= self.fail_max and time.time() - self.break_start < self.reset_timeout:
                await asyncio.sleep(0)
                raise BreakerError
            try:
                result = await f(*args, **kwargs)
                self.fails = 0
                return result
            except Exception as e:
                _ = e
                self.fails += 1
                await asyncio.sleep(self.fails ** 2)
                if self.fails >= self.fail_max:
                    self.break_start = time.time()
            await asyncio.sleep(0)
        return w


brk = Breaker()


class RequestBuilder:
    def __init__(self):
        # Base request characteristics
        self.base_url = 'http://example.com'
        self.user_agent = None
        self.persist_user_agent = True
        # OAuth 1 & 2
        self.client, self.secret, self.token, self.token_secret = None, None, None, None
        self.signature = HmacSha1Signature()
        # OAuth 2
        self.token_url = None
        self.token_expiry = 0
        # Rate limits
        self.rate_limit = 0  # Requests per minute
        self.request_count = 0
        self.last_removed = time.clock()

    def update_rate_limit(self):
        """Decrements request count by number of allowed requests since last update"""
        now = time.clock()
        since = now - self.last_removed
        if since >= 1:
            diff = int(since / 60 * self.rate_limit)
            self.request_count = max(0, self.request_count - diff)
            self.last_removed = now if diff else self.last_removed
            log.debug('Decremented rate limit count by %d', diff)

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

    async def build_request(self, method, url, *, oauth=0, params=None, **aio_kwargs):
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

        aio_kwargs.update({'headers': headers})
        return method, url, params, aio_kwargs


class BaseScraper(RequestBuilder, ABC):
    def __init__(self, output_queue=None):
        super(BaseScraper, self).__init__()
        self.proxy_url = 'http://192.168.1.46:9999'
        self.proxy = None
        self.proxy_params = {'apiKey': 'admin'}
        self.seen = set()
        self.failed = False
        self.queue = output_queue

    @abstractmethod
    async def setup(self, query):
        raise NotImplementedError

    @abstractmethod
    async def handle_response(self,  *args, **kwargs):
        raise NotImplementedError

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
            else:
                log.debug('Removing unclean url: %s', u)

    def filter(self, item):
        """Returns False if the item has been seen before, True if not"""
        hsh = md5(item.encode()).hexdigest()
        if hsh not in self.seen:
            self.seen.add(hsh)
            if len(self.seen) >= 50000:
                self.seen.pop()
            return True
        log.debug('Filtering already seen item: %s', item[:50].replace('\n', ''))
        return False

    @retry(stop_max_attempt_number=3)
    async def update_proxy(self):
        """Updates current proxy with a proxy that has the characteristics specified in params"""
        async with aiohttp.ClientSession() as sess:
            async with sess.get(self.proxy_url, params=self.proxy_params) as resp:
                res = await resp.text()
                try:
                    assert not res.startswith('Error')
                    self.proxy = res
                    log.debug('Successfully updated proxy to %s', res)
                except AssertionError:
                    log.debug('Did not successfully fetch proxy')

    @brk
    @retry(stop_max_attempt_number=8, wait_exponential_multiplier=1.5, wait_jitter_max=500)
    async def _scrape(self, query, *args, use_proxy=False, setup=None, resp_handler=None):
        """Scrapes resources provided in setup method and handles response with handle_response method"""
        setup = setup or self.setup
        resp_handler = resp_handler or self.handle_response
        if use_proxy and (self.proxy is None or self.failed):
            self.update_proxy()
        method, url, params, kwargs = await setup(quote(query))
        async with aiohttp.ClientSession() as sess:
            async with sess.request(method, url, params=params, **kwargs) as resp:
                await resp_handler(resp, *args)
                log.debug('Successfully scraped!\n\tmethod=%s \n\turl=%s, \n\tparams=%s, \n\tkwargs=%s', method, url, str(params), str(kwargs))
                self.failed = False

    async def scrape(self, *args, **kwargs):
        try:
            await self._scrape(*args, **kwargs)
        except BreakerError:
            await asyncio.sleep(30)
        except Exception as e:
            _ = e
            await asyncio.sleep(0)
            self.failed = True
