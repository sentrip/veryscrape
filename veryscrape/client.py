import time
from hashlib import sha1
from random import SystemRandom
from urllib.parse import urljoin

import aioauth_client
import aiohttp
from fake_useragent import UserAgent

from veryscrape import retry

random = SystemRandom().random


class SearchClient:
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

    def __init__(self):
        self.session = aiohttp.ClientSession(headers={'user-agent': UserAgent().random})

    def update_rate_limit(self):
        now = time.time()
        seconds_since = now - self.rate_limit_clock
        if seconds_since >= 1:
            difference = int(seconds_since / 60 * self.rate_limit) if self.rate_limit else 0
            self.request_count = max(0, self.request_count - difference)
            self.rate_limit_clock = now if difference else self.rate_limit_clock

    def build_arguments(self, method, url, oauth, use_proxy, params, aio_kwargs):
        if not url.startswith('http'):
            url = urljoin(self.base_url, url)

        if use_proxy is not None:
            self.update_proxy(use_proxy)
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
        future = await self.session.post(self.token_url, data={'grant_type': 'client_credentials'},
                                         auth=aiohttp.BasicAuth(self.client, self.secret))
        resp = await future.json()
        self.token = resp['access_token']
        try:
            self.token_expiry = int(time.time()) + int(resp['expires_in'])
        except KeyError:
            self.token_expiry = 0

    @retry()
    async def request(self, method, url, params=None, return_json=False, stream=False,
                      oauth=False, use_proxy=None, **aio_kwargs):
        while self.rate_limit_exceeded:
            await self.update_rate_limit()

        if self.oauth2_token_expired:
            await self.update_oauth2_token()

        url, params, aio_kwargs = self.build_arguments(method, url, oauth, use_proxy, params, aio_kwargs)
        resp = await self.session.request(method, url, params=params or {}, **aio_kwargs)
        return resp if stream else (await resp.json() if return_json else await resp.text())

    async def get(self, url, **kwargs):
        return await self.request('GET', url, **kwargs)

    async def post(self, url, data=None, close_response=True, **kwargs):
        resp = await self.request('POST', url, data=data, **kwargs, stream=True)
        if close_response:
            await resp.text()
        return resp

    async def close(self):
        await self.session.close()

    @retry(2, wait_factor=1)
    async def update_proxy(self, proxy_params=None):
        proxy_params = proxy_params or {}
        if self.proxy is None or self.failed:
            resp = await self.session.get(self.proxy_url, params=proxy_params)
            self.proxy = await resp.text()

    @retry(2, wait_factor=1)
    async def send_item(self, content, topic, source):
        return await self.session.post(self.item_url, data={'content': content, 'topic': topic, 'source': source})

    @staticmethod
    def clean_urls(urls):
        bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        new_urls = set()
        for url in urls:
            is_root_url = any(url.endswith(j) for j in bad_domains)
            is_not_relevant = any(j in url for j in false_urls)
            if url.startswith('http') and not (is_root_url or is_not_relevant):
                new_urls.add(url)
        return new_urls
