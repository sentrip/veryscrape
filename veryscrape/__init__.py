import asyncio
import json
import os
import re
import time
from collections import namedtuple
from functools import wraps
from random import SystemRandom

import aiohttp

random = SystemRandom().random

linux_path, windows_path = "/home/djordje/veryscrape/veryscrape",  "C:/users/djordje/desktop/lib"
BASE_DIR = linux_path if os.path.isdir(linux_path) else windows_path
Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


def retry_handler(ex):
    print(repr(ex), 'retry')


def run_handler(ex):
    print(repr(ex), 'run')


def retry(n=5, wait_factor=2):
    def wrapper(fnc):
        async def inner(*args, **kwargs):
            wait, c = 1, 1
            while c <= n:
                try:
                    return await fnc(*args, **kwargs)
                except Exception as e:
                    retry_handler(e)
                    await asyncio.sleep(wait)
                    wait *= wait_factor
                c += 1
            retry_handler(Exception('Function `{}` exceeded maximum allowed number of retries'.format(fnc.__name__)))
        return inner
    return wrapper


def async_run_forever(fnc):
    @wraps(fnc)
    async def wrapper(*args, **kwargs):
        job = asyncio.ensure_future(fnc(*args, **kwargs))
        while True:
            if job.done():
                if job.exception():
                    run_handler(job.exception())
                job = asyncio.ensure_future(fnc(*args, **kwargs))
            await asyncio.sleep(0)
    return wrapper


def synchronous(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        l = asyncio.get_event_loop()
        return l.run_until_complete(f(*args, **kwargs))
    return wrapper


async def get_auth(t):
    api_url = 'http://192.168.0.100:1111'
    async with aiohttp.ClientSession() as sess:
        async with sess.get(api_url, params={'type': t}) as response:
            resp = await response.text()
    try:
        return json.loads(resp)['auth']
    except (KeyError, TypeError, json.JSONDecodeError):
        return []


def load_query_dictionary(file_name):
    """Loads query topics and corresponding queries from disk"""
    queries = {}
    with open(os.path.join(BASE_DIR, 'documents', '%s.txt' % file_name), 'r') as f:
        lns = f.read().splitlines()
        for l in lns:
            x, y = l.split(':')
            queries[x] = y.split(',')
    return queries


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
