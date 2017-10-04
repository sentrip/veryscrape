# Class to download urls from incoming queue and output html in outgoing queue
import asyncio
import re
import time
from contextlib import suppress

import aiohttp

from vs import Item
from vs import SearchClient


class Download(SearchClient):
    def __init__(self, url_queue, result_queue):
        super(Download, self).__init__()
        self.url_queue = url_queue
        self.result_queue = result_queue

    async def download(self, item):
        # (aiohttp.Timeout, aiohttp.ClientError, ssl.CertificateError, KeyError, UnicodeDecodeError)
        with suppress(KeyError, UnicodeDecodeError):
            async with await self.get(item.content) as resp:
                enc_search = re.search('charset=(?P<enc>\S*)', resp.headers.get('content-type', default=''))
                encoding = enc_search.group('enc') if enc_search else 'UTF-8'
                html_text = await resp.text(encoding=encoding)
                if resp.status == 200:
                    await self.result_queue.put(Item(html_text, item.topic, item.source))

    async def stream(self, duration=0):
        start = time.time()
        while not duration or time.time() - start < duration or not self.url_queue.empty():
            try:
                url = self.url_queue.get_nowait()
                asyncio.ensure_future(self.download(url))
            except asyncio.queues.QueueEmpty:
                await asyncio.sleep(0.1)


async def dl(item, sess):
    async with sess.get(item.content) as resp:
        res = await resp.text()
    return Item(res, item.topic, item.source)


async def down(url_queue, result_queue, duration=0):
    session = aiohttp.ClientSession()
    start = time.time()
    jobs = []
    while not duration or time.time() - start < duration or not url_queue.empty():
        item = await url_queue.get()
        jobs.append(dl(item, session))
        if len(jobs) > 100 or url_queue.empty():
            new = await asyncio.gather(*jobs)
            for i in new:
                await result_queue.put(i)
            jobs = []
    await session.close()
