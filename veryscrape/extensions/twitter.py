import asyncio
import json
import re
import time

import aiohttp

from veryscrape import async_run_forever
from veryscrape.client import SearchClient


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


class Twitter(SearchClient):
    base_url = 'https://stream.twitter.com/1.1/'

    # Rate limits
    retry_420 = 60
    snooze_time = 0.25

    def __init__(self, auth):
        super(Twitter, self).__init__()
        self.client, self.secret, self.token, self.token_secret = auth

    async def filter_stream(self, track=None, topic=None, duration=3600, use_proxy=False):
        start_time = time.time()
        raw = await self.post('statuses/filter.json', oauth=1, stream=True,
                              params={'langauge': 'en', 'track': track},
                              use_proxy={'speed': 100, 'https': 1, 'post': 1} if use_proxy else None)
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
            async for status in buffer:
                await self.send_item(status['text'], topic, 'twitter')
                await asyncio.sleep(self.snooze_time)

                if time.time() - start_time >= duration:
                    break
        if not raw.closed:
            raw.close()

    @async_run_forever
    async def stream(self, track=None, topic=None, duration=3600, use_proxy=False):
        await self.filter_stream(track, topic, duration, use_proxy)
