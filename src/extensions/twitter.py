import asyncio
import json
import re
import time
from hashlib import sha1
from random import SystemRandom

import aioauth_client
import aiohttp
from aiohttp.client_exceptions import ClientError, ServerDisconnectedError, ClientPayloadError, TimeoutError as Timeout

from src.base import Item, AsyncStream
from src.extensions.proxy import random_proxy

random = SystemRandom().random


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
        self.sess = None

    async def request(self, method, url, params=None, headers=None, timeout=10, loop=None, **aio_kwargs):
        if not self.sess:
            self.sess = aiohttp.ClientSession()
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


class ReadBuffer(AsyncStream):
    def __init__(self, stream, topic, queue, chunk_size=1024):
        enc_search = re.search('charset=(?P<enc>\S*)', stream.headers.get('content-type', default=''))
        self.encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'
        self.buffer = b''
        self.topic = topic
        self.queue = queue
        self.chunk_size = chunk_size
        self.raw = stream

    async def __anext__(self):
        chunk = None
        try:
            chunk = await self.raw.content.read(self.chunk_size)
        except (Timeout, ClientPayloadError):
            pass
        except Exception as e:
            print('TwitterBuffer', repr(e))
        finally:
            if not chunk:
                raise StopAsyncIteration

        self.buffer += chunk
        ind = self.buffer.find(b'\n')
        if ind > -1:
            status, self.buffer = self.buffer[:ind], self.buffer[ind + 1:]
            if status != b'\r':
                s = json.loads(status.decode(self.encoding))
                if 'limit' in s:
                    sleep_time = (float(s['limit']['track']) + float(
                        s['limit']['timestamp_ms'])) / 1000 - time.time()
                    await asyncio.sleep(sleep_time)
                else:
                    self.queue.put(Item(s['text'], self.topic, 'twitter'))
        return


class QueryStream(AsyncStream):
    def __init__(self, auth, topic, query, queue):
        self.auth = auth
        self.queue = queue
        self.topic = topic
        self.query = query
        self.client = AsyncOAuth(*auth, 'https://stream.twitter.com/1.1/')
        self.params = {'language': 'en', 'track': query}

        self.retry_time_start = 5.0
        self.retry_420_start = 60.0
        self.retry_time_cap = 320.0
        self.snooze_time_step = 0.25
        self.snooze_time_cap = 16
        self.retry_time = self.retry_time_start
        self.snooze_time = self.snooze_time_step

        self.proxy = None
        self.failed = False
        self.proxy_params = {'minDownloadSpeed': '50', 'protocol': 'http', 'allowsHttps': 1, 'allowsPost': 1}

    async def __anext__(self):
        try:
            if not self.proxy or self.failed:
                self.proxy = await random_proxy(**self.proxy_params)
            stream = await self.client.request('POST', 'statuses/filter.json', params=self.params, proxy=self.proxy)
            if stream.status != 200:
                if stream.status == 420:
                    self.retry_time = max(self.retry_420_start, self.retry_time)
                await asyncio.sleep(self.retry_time)
                self.retry_time = min(self.retry_time * 2., self.retry_time_cap)
            else:
                self.retry_time = self.retry_time_start
                self.snooze_time = self.snooze_time_step
                await ReadBuffer(stream, self.topic, self.queue).stream()
                await asyncio.sleep(self.snooze_time)
                self.failed = False
        except ClientError:
            self.client.close()
            self.client = AsyncOAuth(*self.auth, 'https://stream.twitter.com/1.1/')
            await asyncio.sleep(self.retry_time)
        except (ServerDisconnectedError, Timeout):
            await asyncio.sleep(self.retry_time)
        except Exception as e:
            self.failed = True
            print('Twitter', repr(e))
            self.client.close()
            self.client = AsyncOAuth(*self.auth, 'https://stream.twitter.com/1.1/')
            await asyncio.sleep(self.retry_time)
        return


class TweetStream(AsyncStream):
    def __init__(self, auth, topic, queries, queue):
        self.auth = auth
        self.topic = topic
        self.queries = queries
        self.queue = queue

    async def __anext__(self):
        jobs = []
        for query in self.queries:
            jobs.append(QueryStream(self.auth, self.topic, query, self.queue).stream())
        await asyncio.gather(*jobs)
