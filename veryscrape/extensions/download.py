# Class to download urls from incoming queue and output html in outgoing queue
import asyncio
import re
import time
from contextlib import suppress

from veryscrape import Item
from veryscrape.client import SearchClient


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
