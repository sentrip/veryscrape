import asyncio
import json
import random as rd
import re
import time
from hashlib import sha1
from random import SystemRandom

import aioauth_client
import aiohttp

from base import Item

random = SystemRandom().random


class RandomExponentialBackOff:
    def __init__(self):
        self.count = 0
        self.retry_time = 1

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.count:
            await asyncio.sleep(self.retry_time + self.retry_time / 4 - rd.random() * self.retry_time / 2)
            self.retry_time *= 2
        self.count += 1
        return self.count


class AsyncOAuth(aioauth_client.Client):
    access_token_key = 'oauth_token'
    request_token_url = None
    version = '1.0'

    def __init__(self, consumer_key, consumer_secret, oauth_token=None, oauth_token_secret=None,
                 base_url=None, signature=None, **params):
        super().__init__(base_url, None, None, None, None)

        self.oauth_token = oauth_token
        self.oauth_token_secret = oauth_token_secret
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.params = params
        self.signature = signature or aioauth_client.HmacSha1Signature()
        self.sess = aiohttp.ClientSession()

    async def request(self, method, url, params=None, headers=None, timeout=10, loop=None, **aio_kwargs):
        oparams = {
            'oauth_consumer_key': self.consumer_key,
            'oauth_nonce': sha1(str(random()).encode('ascii')).hexdigest(),
            'oauth_signature_method': self.signature.name,
            'oauth_timestamp': str(int(time.time())),
            'oauth_version': self.version}
        oparams.update(params or {})
        if self.oauth_token:
            oparams['oauth_token'] = self.oauth_token

        url = self._get_url(url)
        oparams['oauth_signature'] = self.signature.sign(self.consumer_secret, method, url,
                                                         oauth_token_secret=self.oauth_token_secret, **oparams)

        return await self.sess.request(method, url, params=oparams, headers=headers, **aio_kwargs)

    def close(self):
        self.sess.close()


class ReadBuffer:
    def __init__(self, stream, topic, queue, chunk_size=1024):
        enc_search = re.search('charset=(?P<enc>\S*)', stream.headers.get('content-type', default=''))
        self.encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'
        self.buffer = b''
        self.topic = topic
        self.queue = queue
        self.chunk_size = chunk_size
        self.raw = stream

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            chunk = await self.raw.content.read(self.chunk_size)
        except TimeoutError:
            return StopAsyncIteration
        except Exception as e:
            print('TwitterBuffer' + repr(e))
            return StopAsyncIteration
        if not chunk:
            return StopAsyncIteration
        self.buffer += chunk
        ind = self.buffer.find(b'\n')
        if ind > -1:
            status, self.buffer = self.buffer[:ind], self.buffer[ind + 1:]
            if status != b'\r':
                s = json.loads(status.decode(self.encoding))
                if 'limit' in s:
                    sleep_time = (float(s['limit']['track']) + float(
                        s['limit']['timestamp_ms'])) / 1000 - time.time()
                    await asyncio.sleep(sleep_time * 2)
                else:
                    self.queue.put(Item(s['text'], self.topic, 'twitter'))
        return


class QueryStream:
    def __init__(self, auth, topic, query, queue):
        self.auth = auth
        self.queue = queue
        self.topic = topic
        self.query = query
        self.client = None
        self.params = {'language': 'en', 'track': query}

        self.retry_time = 5.0
        self.retry_420 = 60.0
        self.snooze_time = 0.25

        self.retry_counter = RandomExponentialBackOff()
        self.buffer = None
        self.raw = None
        self.is_streaming = False
        self.is_reading = False

    async def stream(self):
        self.client = AsyncOAuth(*self.auth, 'https://stream.twitter.com/1.1/')
        async for _ in self.retry_counter:
            try:
                self.raw = await self.client.request('POST', 'statuses/filter.json', params=self.params)
                if self.raw.status == 420:
                    await asyncio.sleep(self.retry_420)
                elif self.raw.status != 200:
                    await asyncio.sleep(self.retry_time)
                else:
                    self.buffer = ReadBuffer(self.raw, self.topic, self.queue)
                    self.retry_counter.count = 0
                    async for _ in self.buffer:
                        pass
            except Exception as e:
                print('Twitter', repr(e))
        self.client.close()
