import asyncio
import json
import re
import time
from contextlib import suppress
from hashlib import sha1
from random import SystemRandom

import aioauth_client
import aiohttp
from aiohttp.client_exceptions import ClientHttpProxyError, ClientConnectorError, ServerDisconnectedError

from base import Item

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


async def async_stream_read_loop(parent, stream, topic, chunk_size=1024):
    buffer = b''
    charset = stream.headers.get('content-type', default='')
    enc_search = re.search('charset=(?P<enc>\S*)', charset)
    encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'

    while True:
        try:
            chunk = await stream.content.read(chunk_size)
        except Exception as e:
            print('' + repr(e))
            break
        if not chunk:
            break
        buffer += chunk
        ind = buffer.find(b'\n')
        if ind > -1:
            status, buffer = buffer[:ind], buffer[ind+1:]
            with suppress(json.JSONDecodeError):
                s = json.loads(status.decode(encoding))
                await asyncio.sleep(0)
                if 'limit' in s:
                    sleep_time = (float(s['limit']['track']) + float(s['limit']['timestamp_ms']))/1000 - time.time()
                    await asyncio.sleep(sleep_time)
                else:
                    parent.result_queue.put(Item(s['text'], topic, 'twitter'))


async def twitter(parent, topic, query, proxy_thread):
    """Asynchronous twitter stream - streams tweets for provided query, topic is used for categorization"""
    retry_time_start = 5.0
    retry_420_start = 60.0
    retry_time_cap = 320.0
    snooze_time_step = 0.25
    snooze_time_cap = 16
    retry_time = retry_time_start
    snooze_time = snooze_time_step
    auth = parent.twitter_authentications[topic]
    client = AsyncOAuth(*auth, 'https://stream.twitter.com/1.1/')
    p = {'language': 'en', 'track': query}
    proxy = await proxy_thread.random_async('twitter', False)
    while parent.running:
        try:
            stream = await client.request('POST', 'statuses/filter.json', params=p, proxy=proxy)
        except (ClientHttpProxyError, ClientConnectorError, ServerDisconnectedError):
            proxy = await proxy_thread.random_async('twitter', False)
            client.close()
            client = AsyncOAuth(*auth, 'https://stream.twitter.com/1.1/')
        except Exception as e:
            print('Twitter', repr(e))
            proxy = await proxy_thread.random_async('twitter', False)
            await asyncio.sleep(retry_time * 2)
            client.close()
            client = AsyncOAuth(*auth, 'https://stream.twitter.com/1.1/')
            continue

        if stream.status != 200:
            if stream.status == 420:
                retry_time = max(retry_420_start, retry_time)
            await asyncio.sleep(retry_time)
            retry_time = min(retry_time * 2., retry_time_cap)
        else:
            retry_time = retry_time_start
            snooze_time = snooze_time_step
            await async_stream_read_loop(parent, stream, topic)

        await asyncio.sleep(snooze_time)
        snooze_time = min(snooze_time + snooze_time_step, snooze_time_cap)
    client.close()
