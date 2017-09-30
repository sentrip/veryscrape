import asyncio
import json
import os
import re
import time
from collections import namedtuple, defaultdict
from functools import wraps, partial
from random import SystemRandom

import aiohttp

random = SystemRandom().random

linux_path, windows_path = "/home/djordje/veryscrape/veryscrape/documents",  "C:/users/djordje/desktop/lib/documents"
BASE_DIR = linux_path if os.path.isdir(linux_path) else windows_path
Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


def retry_handler(ex, test=False):
    if not test:
        print(repr(ex), 'retry')


def run_handler(ex):
    print(repr(ex), 'run')


def retry(n=5, wait_factor=2, initial_wait=1, test=False):
    def wrapper(fnc):
        async def inner(*args, **kwargs):
            wait, c = initial_wait, 1
            while c <= n:
                try:
                    return await fnc(*args, **kwargs)
                except Exception as e:
                    retry_handler(e, test)
                    await asyncio.sleep(wait)
                    wait *= wait_factor
                c += 1
            raise TimeoutError('Function `{}` exceeded maximum allowed number of retries'.format(fnc.__name__))
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
    with open(os.path.join(BASE_DIR, '%s.txt' % file_name), 'r') as f:
        lns = f.read().splitlines()
        for l in lns:
            x, y = l.split(':')
            queries[x] = y.split(',')
    return queries


def queue_filter(queue, interval=60):
        data = defaultdict(partial(defaultdict, list))
        start = time.time()
        while True:
            if not queue.empty():
                item = queue.get_nowait()
                data[item.topic][item.source].append(item.content)
            if time.time() - start >= interval:
                start = time.time()
                averages = {k: {t: sum(s)/max(1, len(s)) for t, s in qs.items()} for k, qs in data.items()}
                yield averages
                for k, qs in data.items():
                    for t in qs:
                        data[k][t] = []
