import asyncio
import json
import random
import re
import time
from hashlib import sha1
from urllib.parse import urljoin

import aioauth_client
import aiohttp
from fake_useragent import UserAgent


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

    async def safely_execute(self, f, retries=20, *args, **kwargs):
        executed = False
        e = None
        while not executed:
            if await self.__anext__() < retries:
                try:
                    result = await f(*args, **kwargs)
                    self.reset()
                    return result
                except Exception as e:
                    print(repr(e))
            else:
                break
        raise ValueError('Safe execution of {} failed, last known cause: {}'.format(f.__name__, repr(e)))


class ReadBuffer:
    def __init__(self, stream, chunk_size=1024):
        enc_search = re.search('charset=(?P<enc>\S*)', stream.headers.get('content-type', default=''))
        self.encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'
        self.buffer = b''
        self.chunk_size = chunk_size
        self.raw = stream

    def __aiter__(self):
        return self

    async def next_chunk(self):
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
        status = json.loads(item).decode(self.encoding)
        if 'limit' in status:
            await asyncio.sleep((float(status['limit']['track']) + float(
                status['limit']['timestamp_ms'])) / 1000 - time.time())
            item = await self.next_item()
            status = json.loads(item).decode(self.encoding)
        return status


class SearchClient:
    base_url = None

    # OAuth
    client, secret, token, token_secret = None, None, None, None
    signature = None

    def __init__(self):
        self.session = aiohttp.ClientSession(headers={'user-agent': UserAgent().random})
        self.random = random.SystemRandom().random
        self.retries = ExponentialBackOff()

    @property
    def oauth_parameters(self):
        return dict(
            oauth_consumer_key=self.client,
            oauth_token=self.token,
            oauth_signature_method=self.signature.name,
            oauth_nonce=sha1(str(self.random()).encode('ascii')).hexdigest(),
            oauth_version='1.0',
            oauth_timestamp=str(int(time.time()))
        )

    async def _request(self, method, url, params=None, oauth=False, use_base_url=True, **aio_kwargs):
        url = urljoin(self.base_url, url) if use_base_url else url
        params = params or {}
        if oauth:
            params.update(self.oauth_parameters)
            params['oauth_signature'] = self.signature.sign(self.client, method, url, self.token_secret, **params)
        async with self.session.request(method, url, params=params, **aio_kwargs) as response:
            await response.text()
            return response

    async def request(self, method, url, params=None, oauth=False, **aio_kwargs):
        return await self.retries.safely_execute(self._request, method, url, params=params, oauth=oauth, **aio_kwargs)

    async def random_proxy(self, **params):
        kwargs = {'json': json.dumps(params)} if params else {}
        resp = await self.request('GET', 'http://192.168.0.100:9999', use_base_url=False, **kwargs)
        if resp.status != 200:
            raise BrokenPipeError('Could not connect to proxy server')
        else:
            return resp

    async def send_item(self, content, topic, source):
        j = json.dumps({'content': content, 'topic': topic, 'source': source})
        resp = await self.request('GET', 'http://192.168.0.102:9999', use_base_url=False, json=j)
        if resp.status != 200:
            raise BrokenPipeError('Could not connect to item server')
        else:
            return resp

    def close(self):
        self.session.close()


class Twitter(SearchClient):
    base_url = 'https://stream.twitter.com/1.1/'
    signature = aioauth_client.HmacSha1Signature()

    # Rate limits
    retry_420 = 60
    snooze_time = 0.25

    def __init__(self, auth):
        super(Twitter, self).__init__()
        self.client, self.secret, self.token, self.token_secret = auth
        self.proxy = None
        self.failed = False

    async def filter_stream(self, track=None, topic=None):
        params = {'langauge': 'en', 'track': track}
        proxy_params = {'speed': 100, 'https': 1, 'post': 1}

        if not self.proxy or self.failed:
            self.proxy = await self.random_proxy(**proxy_params)
        raw = await self.request('POST', 'statuses/filter.json', params=params, proxy=self.proxy)

        if raw.status == 420:
            self.snooze_time += 0.5
            await asyncio.sleep(self.retry_420)
        elif raw.status != 200:
            self.failed = True
            self.snooze_time += 0.5
            raise ValueError('Incorrect stream response!')
        else:
            self.failed = False
            self.snooze_time = 0.25
            buffer = ReadBuffer(raw)
            async for item in buffer:
                content = (await item)['text']
                self.send_item(content, topic, 'twitter')
                await asyncio.sleep(self.snooze_time)
