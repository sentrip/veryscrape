import asyncio
import json
import os
import re
import time
from collections import namedtuple
from hashlib import sha1
from random import SystemRandom
from urllib.parse import urljoin

import aioauth_client
import aiohttp
from fake_useragent import UserAgent

random = SystemRandom().random

BASE_DIR = "/home/djordje/Sentrip/" if os.path.isdir("/home/djordje/Sentrip/") else "C:/users/djordje/desktop/"
Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


class ExponentialBackOff:
    def __init__(self, ratio=2):
        self.ratio = ratio
        self.count = 0
        self.retry_time = 1

    def reset(self):
        self.count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.count:
            await asyncio.sleep(self.retry_time)
            self.retry_time *= self.ratio
        self.count += 1
        return self.count

    async def safely_execute(self, f, *args, **kwargs):
        retries = kwargs.get('retries', 5)
        executed = False
        ex, excep = '', None
        while not executed:
            if await self.__anext__() <= retries:
                try:
                    result = await f(*args, **kwargs)
                    self.reset()
                    return result
                except Exception as e:
                    ex += repr(e) + '\n'
                    print(repr(e))
                    excep = e
            else:
                break
        if not excep:
            raise ValueError('Safe execution of {} failed, last known cause: {}'.format(f.__name__, ex))
        else:
            raise excep


class ReadBuffer:
    def __init__(self, stream, chunk_size=1024):
        enc_search = re.search('charset=(?P<enc>\S*)', stream.headers.get('content-type', ''))
        self.encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'
        self.buffer = b''
        self.chunk_size = chunk_size
        self.raw = stream

    def __aiter__(self):
        return self

    async def next_chunk(self):
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
        item = await self.next_item()
        status = json.loads(item.decode(self.encoding))
        if 'limit' in status:
            await asyncio.sleep((float(status['limit']['track']) + float(
                status['limit']['timestamp_ms'])) / 1000 - time.time())
            status = await self.__anext__()
        return status


class SearchClient:
    base_url = None

    # OAuth
    client, secret, token, token_secret = None, None, None, None
    token_url = None
    token_expiry = 0
    signature = aioauth_client.HmacSha1Signature()
    # Proxies and retries
    proxy = None
    failed = False
    retries = ExponentialBackOff()
    # Rate limits
    rate_limit = 0  # Requests per minute
    request_count = 0
    rate_limit_clock = time.time()

    def __init__(self):
        self.session = aiohttp.ClientSession(headers={'user-agent': UserAgent().random})

    @property
    def oauth1_parameters(self):
        return {
            'oauth_consumer_key': self.client,
            'oauth_token': self.token,
            'oauth_signature_method': self.signature.name,
            'oauth_nonce': sha1(str(random()).encode('ascii')).hexdigest(),
            'oauth_version': '1.0',
            'oauth_timestamp': str(int(time.time()))
        }

    async def _request(self, method, url, params, oauth, use_proxy, **aio_kwargs):
        params = params or {}

        if (self.proxy is None or self.failed) and use_proxy is not None:
            kwargs = {} if use_proxy is None else {'json':  json.dumps(use_proxy)}
            self.proxy = await self.request('GET', 'http://192.168.0.100:9999', **kwargs)
            aio_kwargs.update({'proxy': self.proxy})

        if oauth == 1:
            params.update(self.oauth1_parameters)
            params['oauth_signature'] = self.signature.sign(self.secret, method, url, self.token_secret, **params)

        elif oauth == 2:
            if (time.time() >= self.token_expiry and self.token_expiry) or self.token is None:
                await self.update_oauth2_token()
            aio_kwargs.update({'headers': {'Authorization': 'bearer ' + self.token}})

        return await self.session.request(method, url, params=params, **aio_kwargs)

    async def request(self, method, url, params=None, oauth=False, stream=False,
                      use_proxy=None, return_json=False, **aio_kwargs):
        url = urljoin(self.base_url, url) if not any(url.startswith(pre) for pre in ['http://', 'https://']) else url

        while self.rate_limit and self.request_count > self.rate_limit:
            await self.update_rate_limit()

        resp = await self.retries.safely_execute(self._request, method, url, params, oauth, use_proxy, **aio_kwargs)

        if resp is None or resp.status != 200:
            raise ConnectionError('Could not {} {}, error code: {}'.format(method, url,
                                                                           'None' if resp is None else resp.status))
        if stream:
            return resp
        elif return_json:
            return await resp.json()
        else:
            return await resp.text()

    async def update_oauth2_token(self):
        resp = await self.request('POST', self.token_url, return_json=True,
                                  data={'grant_type': 'client_credentials'},
                                  auth=aiohttp.BasicAuth(self.client, self.secret))
        self.token = resp['access_token']
        try:
            self.token_expiry = int(time.time()) + int(resp['expires_in'])
        except KeyError:
            self.token_expiry = 0

    async def update_rate_limit(self):
        now = time.time()
        seconds_since = now - self.rate_limit_clock
        if seconds_since >= 1:
            difference = int(seconds_since / 60 * self.rate_limit)
            self.request_count -= difference
            self.request_count = max(0, self.request_count)
            self.rate_limit_clock = now if difference else self.rate_limit_clock
        await asyncio.sleep(0.1)

    async def send_item(self, content, topic, source):
        return await self.request('POST', 'http://192.168.1.53:9999',
                                  data={'content': content, 'topic': topic, 'source': source})

    async def close(self):
        try:
            await self.session.close()
        except TypeError:
            self.session.close()


class Producer:
    @staticmethod
    def load_query_dictionary(file_name):
        """Loads query topics and corresponding queries from disk"""
        queries = {}
        with open(os.path.join(BASE_DIR, 'lib', 'documents', file_name), 'r') as f:
            lns = f.read().splitlines()
            for l in lns:
                x, y = l.split(':')
                queries[x] = y.split(',')
        return queries

    @staticmethod
    def load_authentications(file_name):
        """Load api keys seperated by '|' from file"""
        topics = Producer.load_query_dictionary('query_topics.txt')
        api_keys = {}
        with open(os.path.join(BASE_DIR, 'lib', 'api', file_name), 'r') as f:
            data = f.read().splitlines()
            for i, topic in enumerate(topics):
                api_keys[topic] = data[i].split('|')
        return api_keys
