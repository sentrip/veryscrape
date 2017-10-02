import time
from collections import namedtuple
from hashlib import sha1
from random import SystemRandom
from urllib.parse import urljoin

import aioauth_client
import aiohttp
from fake_useragent import UserAgent

from veryscrape import retry

random = SystemRandom().random
mock_response = namedtuple('Response', ['status', 'text'])


class SearchClient(aiohttp.ClientSession):
    base_url = 'http://google.com'
    # OAuth
    client, secret, token, token_secret = None, None, None, None
    token_url = None
    token_expiry = 0
    signature = aioauth_client.HmacSha1Signature()
    # Proxy and item servers
    proxy_url = 'http://192.168.0.100:9999'
    item_url = 'http://192.168.1.53:9999'
    # Rate limits
    rate_limit = 0  # Requests per minute
    request_count = 0
    rate_limit_clock = time.time()
    proxy = None
    failed = False
    user_agent = UserAgent().random

    def __init__(self):
        super(SearchClient, self).__init__()

    def update_rate_limit(self):
        now = time.time()
        seconds_since = now - self.rate_limit_clock
        if seconds_since >= 1:
            difference = int(seconds_since / 60 * self.rate_limit) if self.rate_limit else 0
            self.request_count = max(0, self.request_count - difference)
            self.rate_limit_clock = now if difference else self.rate_limit_clock

    async def build_arguments(self, method, url, oauth, use_proxy, params, aio_kwargs):
        if not url.startswith('http'):
            url = urljoin(self.base_url, url)

        if use_proxy is not None:
            await self.update_proxy(use_proxy)
            aio_kwargs.update({'proxy': self.proxy})

        if oauth == 1:
            params.update(self.oauth1_parameters)
            params['oauth_signature'] = self.signature.sign(self.secret, method, url,
                                                            self.token_secret, **params)
        elif oauth == 2:
            aio_kwargs.update(self.oauth2_parameters)

        return url, params, aio_kwargs

    @property
    def oauth1_parameters(self):
        return {'oauth_consumer_key': self.client,
                'oauth_token': self.token,
                'oauth_signature_method': self.signature.name,
                'oauth_nonce': sha1(str(random()).encode('ascii')).hexdigest(),
                'oauth_timestamp': str(int(time.time())),
                'oauth_version': '1.0'}

    @property
    def oauth2_parameters(self):
        return {'headers': {'Authorization': 'bearer ' + self.token}}

    @property
    def oauth2_token_expired(self):
        return (time.time() >= self.token_expiry or self.token is None) and self.token_expiry

    @property
    def rate_limit_exceeded(self):
        return self.rate_limit and self.request_count > self.rate_limit

    @retry()
    async def update_oauth2_token(self):
        async with self.request('POST', self.token_url, data={'grant_type': 'client_credentials'},
                                auth=aiohttp.BasicAuth(self.client, self.secret)) as resp:
            auth = await resp.json()
            self.token = auth['access_token']
            try:
                self.token_expiry = int(time.time()) + int(auth['expires_in'])
            except KeyError:
                self.token_expiry = 0

    async def build_request(self, method, url, oauth, use_proxy, params, aio_kwargs):
        while self.rate_limit_exceeded:
            await self.update_rate_limit()

        if self.oauth2_token_expired:
            await self.update_oauth2_token()

        return await self.build_arguments(method, url, oauth, use_proxy, params, aio_kwargs)

    @retry()
    async def get(self, url, *, allow_redirects=True, params=None, oauth=False, use_proxy=None, **kwargs):
        url, params, aio_kwargs = await self.build_request('GET', url, oauth, use_proxy, params or {}, kwargs)
        kwargs.update(aio_kwargs)
        return aiohttp.client._RequestContextManager(self._request('GET', url, allow_redirects=allow_redirects,
                                                                   params=params, **kwargs))

    @retry()
    async def post(self, url, data, params=None, oauth=False, use_proxy=None, **kwargs):
        url, params, aio_kwargs = await self.build_request('POST', url, oauth, use_proxy, params or {}, kwargs)
        kwargs.update(aio_kwargs)
        return aiohttp.client._RequestContextManager(self._request('POST', url, data=data, params=params, **kwargs))

    @retry()
    async def update_proxy(self, proxy_params=None):
        proxy_params = proxy_params or {}
        if self.proxy is None or self.failed:
            async with await self.get(self.proxy_url, params=proxy_params) as resp:
                res = await resp.text()
            if not res.startswith('Incorrect'):
                self.proxy = res
            else:
                raise ConnectionError

    @staticmethod
    def clean_urls(urls):
        bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        for url in list(urls):
            is_root_url = any(url.endswith(j) for j in bad_domains)
            is_not_relevant = any(j in url for j in false_urls)
            if is_root_url or is_not_relevant or not url.startswith('http'):
                urls.remove(url)
        return urls
