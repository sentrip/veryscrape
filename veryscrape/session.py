from aioauth_client import HmacSha1Signature
from aiohttp.client import _RequestContextManager
from collections import OrderedDict
from fake_useragent import UserAgent, settings, FakeUserAgentError
from hashlib import sha1
from time import time
from random import SystemRandom
from urllib.parse import urljoin, urlparse
import asyncio
import aiohttp
import logging
import re


log = logging.getLogger(__name__)
random = SystemRandom().random
# If fake_useragent can't connect then this allows it to finish early
settings.HTTP_DELAY = 0.01
settings.HTTP_RETRIES = 1
try:
    _agent_factory = UserAgent()
except FakeUserAgentError:  # pragma: nocover
    _agent_factory = type('', (), {'random': 'no-agent'})


def _create_nested_metadata(parent):
    request_metadata = {}
    for path, limit in parent.items():
        if isinstance(limit, dict):
            request_metadata[path] = _create_nested_metadata(limit)
        else:
            request_metadata[path] = {'n': 0, 'last': time()}
    return request_metadata


class RateLimiter:
    def __init__(self, rate_limits, period):
        self.rate_limit_period = period
        self.rate_limits = OrderedDict()
        defined_limits = rate_limits.copy()
        global_limit = defined_limits.pop('*', None)
        # sorts paths alphabetically for deterministic execution pre-python3.6
        for k in sorted(list(defined_limits.keys())):
            self.rate_limits[k] = defined_limits[k]
        # set global rate limit last so it is always checked last
        if global_limit:
            assert isinstance(global_limit, int), \
                'Global rate limit must be defined directly with an integer'
            self.rate_limits['*'] = global_limit
        self._rl_meta = _create_nested_metadata(self.rate_limits)

    def get_limit(self, url, parent=None, metadata=None):
        parent = parent or self.rate_limits
        metadata = metadata or self._rl_meta
        for path, limit in parent.items():
            if re.search(path, url):
                if isinstance(limit, dict):
                    return self.get_limit(url, parent=limit,
                                          metadata=metadata[path])
                return limit, metadata[path]
        else:
            return False, {}

    def refresh_limits(self, rate_limit, metadata):
        time_since = time() - metadata['last']
        metadata['n'] = max(0, metadata['n'] - int(
            time_since * rate_limit / self.rate_limit_period))

    async def _wait_limit(self, rate_limit, metadata):
        while metadata['n'] >= rate_limit:
            self.refresh_limits(rate_limit, metadata)
            # wait 1/1000 the time of one request to try again
            await asyncio.sleep(self.rate_limit_period / rate_limit / 1000)

    async def wait_limit(self, url):
        rate_limit, metadata = self.get_limit(url)
        if rate_limit:
            self.refresh_limits(rate_limit, metadata)
            await self._wait_limit(rate_limit, metadata)
            metadata['last'] = time()
            metadata['n'] += 1


class OAuth1:
    def __init__(self, client, secret, token, token_secret):
        self.signature = HmacSha1Signature()
        self.client = client
        self.secret = secret
        self.token = token
        self.token_secret = token_secret

    @property
    def oauth_params(self):
        """
        Returns dictionary of oauth1 parameters
        required for oauth1 signed http request
        """
        return {
            'oauth_consumer_key': self.client,
            'oauth_token': self.token,
            'oauth_signature_method': self.signature.name,
            'oauth_nonce': sha1(str(random()).encode('ascii')).hexdigest(),
            'oauth_timestamp': str(int(time())),
            'oauth_version': '1.0'
        }

    async def patch_request(self, method, url, params, kwargs):
        params.update(self.oauth_params)
        params['oauth_signature'] = self.signature.sign(
            self.secret, method, url, self.token_secret, **params)
        return url, params, kwargs


class OAuth2:
    def __init__(self, client, secret, token_url):
        self.client = client
        self.secret = secret
        self.token_url = token_url
        self.auth = aiohttp.BasicAuth(self.client, self.secret)
        self.token = None
        self.token_expiry = 0

    @property
    def oauth2_token_expired(self):
        """Returns true if current oauth2 token needs to be refreshed"""
        return (
            self.token is None
            or (
                self.token_expiry
                and time() >= self.token_expiry
            )
        )

    async def auth_token(self):
        """
        Returns dictionary of oauth2 parameters
        required for oauth2 signed http request
        """
        if self.oauth2_token_expired:
            async with aiohttp.ClientSession() as sess:
                async with sess.post(
                        self.token_url,
                        data={'grant_type': 'client_credentials'},
                        auth=self.auth
                ) as resp:
                    auth = await resp.json()
            self.token = auth['access_token']
            try:
                self.token_expiry = int(time()) + int(auth['expires_in'])
            except KeyError:
                self.token_expiry = 0
        return {'Authorization': 'bearer ' + self.token}

    async def patch_request(self, method, url, params, kwargs):
        auth = await self.auth_token()
        if kwargs.get('headers', None) is None:
            kwargs['headers'] = {}
        kwargs['headers'].update(auth)
        return url, params, kwargs


class Session:
    rate_limits = {}
    rate_limit_period = 60

    persist_user_agent = True
    user_agent = None

    def __init__(self, proxy_pool=None):
        self.limiter = RateLimiter(self.rate_limits, self.rate_limit_period)
        self._pool = proxy_pool
        self._session = aiohttp.ClientSession()
        # This is so you can call get, post, etc... without having to recode
        # aiohttp uses _request internally for everything, so that is saved,
        # and calls to aiohttp's _request are sent to _request of this class,
        # which uses the saved _request at the end of the method
        self._original_request = self._session._request
        self._session.__dict__['_request'] = self._request

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return self._session.__getattribute__(item)

    async def __aenter__(self):
        self._session = await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._session.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def _user_agent(self):
        """
        Creates new random user agent, sets self.user agent and returns,
        otherwise returns current user agent
        """
        if self.user_agent is None or not self.persist_user_agent:
            self.user_agent = _agent_factory.random
            return self.user_agent
        else:
            return self.user_agent

    async def _request(self, method, url, **kwargs):
        await self.limiter.wait_limit(url)
        if kwargs.get('headers', None) is None:
            kwargs['headers'] = {}
        kwargs['headers'].update({'user-agent': self._user_agent})

        if self._pool is not None:
            proxy = await self._pool.get(scheme=urlparse(url).scheme)
            kwargs.update(proxy='http://%s:%d' % (proxy.host, proxy.port))

        log.debug('Requesting: \n\tMETHOD=%s, \n\tURL=%s, \n\tKWARGS=%s',
                  method, url, str(kwargs))
        return await self._original_request(method, url, **kwargs)

    def request(self, method, url, **kwargs):
        return _RequestContextManager(self._request(method, url, **kwargs))


class OAuth1Session(Session):
    base_url = None
    _patcher = OAuth1

    def __init__(self, *args, **kwargs):
        super(OAuth1Session, self).__init__(**kwargs)
        self.patcher = self._patcher(*args)

    async def _request(self, method, url, **kwargs):
        if self.base_url is not None and not url.startswith('http'):
            url = urljoin(self.base_url, url)
        url, params, kwargs = await self.patcher.patch_request(
            method, url, kwargs.pop('params', None) or {}, kwargs
        )
        return await super(OAuth1Session, self)._request(
            method, url, params=params, **kwargs
        )


class OAuth2Session(OAuth1Session):
    _patcher = OAuth2


async def fetch(method, url, *, params=None,
                session=None, stream_func=None, **kwargs):
    if session is None:
        sess = aiohttp.ClientSession()
    else:
        sess = session
    result = ''
    timeout = kwargs.pop('timeout', 10)
    kwargs.update(params=params, timeout=timeout)
    async with sess.request(method, url, **kwargs) as resp:
        try:
            resp.raise_for_status()
            if stream_func is not None:
                async for line in resp.content:
                    stream_func(line)
                    await asyncio.sleep(0)
            else:
                result = await resp.text()
        except aiohttp.ClientResponseError:
            error_text = await resp.text()
            log.error(
                'ERROR %d requesting %-30s - %s', resp.status, url,
                error_text[:100].replace('\n', '')
            )
        except Exception as e:
            log.exception("ERROR requesting %-30s: %s", url, repr(e))

    if session is None:
        await sess.close()

    return result
