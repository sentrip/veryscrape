import asyncio
import random
import re
import ssl
import time

import aiohttp
from retrying import retry

from veryscrape.api import Item, BaseScraper
from vstest import synchronous


@synchronous
async def get_topics(q):
    b = BaseScraper()
    r = await b.get_api_data(q)
    return r


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_jitter_max=500)
async def fetch(url, session):
    """Tries to download html for a single url with provided session"""
    try:
        async with session.get(url) as raw:
            enc_search = re.search('charset=(?P<enc>\S*)', raw.headers.get('content-type', default=''))
            encoding = enc_search.group('enc') if enc_search else 'UTF-8'
            return await raw.text(encoding=encoding, errors='ignore')
    except (aiohttp.ClientError, aiohttp.ServerDisconnectedError):
        return None


async def download(url_queue, result_queue, duration=0):
    """Continuously downloads urls placed in url queue and places resulting htmls into result queue"""
    jobs = []
    start = time.time()
    sess = aiohttp.ClientSession()
    while not duration or time.time() - start < duration or not url_queue.empty():
        if len(jobs) >= 100 or url_queue.empty():
            responses = await asyncio.gather(*jobs)
            jobs = []
            for resp in responses:
                if resp is not None:
                    new_item = Item(resp, item.topic, item.source)
                    await result_queue.put(new_item)
        item = await url_queue.get()
        jobs.append(fetch(item.content, sess))
    await sess.close()


def scrape_handler(ex):
    """Exception handler for circuit broken wrapper"""
    allowed = [aiohttp.ClientError, aiohttp.ServerDisconnectedError, aiohttp.ClientOSError,
               ssl.CertificateError, ConnectionError, KeyError]
    for a_ex in allowed:
        if isinstance(ex, a_ex):
            return
    else:
        print(repr(ex))


def circuit_broken(n=5, reset=10, ex_handler=lambda ex: print(repr(ex))):
    """Repeats a task infinitely, implementing the circuit breaker pattern"""
    def outer_wrapper(f):
        async def inner_wrapper(*args, **kwargs):
            offset = reset * random.random() * 0.2
            last_fail = 0.
            count = 0
            while True:
                await asyncio.sleep(0)
                if count < n:
                    try:
                        await f(*args, **kwargs)
                        count = 0
                    except Exception as e:
                        ex_handler(e)
                        count += 1
                        last_fail = time.time()
                elif count >= n and time.time() - last_fail > reset + offset:
                    count -= 1
        return inner_wrapper
    return outer_wrapper


class InfiniteScraper:
    def __init__(self, cls, *args, **kwargs):
        self.cls = cls(*args, **kwargs)

    @circuit_broken(n=5, reset=30, ex_handler=scrape_handler)
    async def scrape_forever(self, start_delay, repeat_every, *args, **kwargs):
        start = time.time()
        await asyncio.sleep(start_delay)
        await self.cls.scrape(*args, **kwargs)
        await asyncio.sleep(max(0, repeat_every - (time.time() - start)))
