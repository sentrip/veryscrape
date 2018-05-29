from concurrent.futures import ProcessPoolExecutor
from functools import partial
import asyncio
import json
import logging
import signal

from proxybroker import Broker, ProxyPool

from .items import ItemMerger
from .process import clean_item
from .scrapers import Reddit, Twitter, Google, Twingly


log = logging.getLogger('veryscrape')
SCRAPERS = {
    'reddit': Reddit,
    'twitter': Twitter,
    'article': Google,
    'blog': Twingly
}


class VeryScrape:
    def __init__(self, q, config=None, loop=None, n_cores=1):
        self.queue = q
        self.loop = loop or asyncio.get_event_loop()
        self.pool = ProcessPoolExecutor(n_cores)
        self.gens = []
        self.scrapers = []
        self.items = None
        self.using_proxies = False

        if isinstance(config, str):
            self.load_config(config)
        else:
            self.config = config or {}

        proxy_queue = asyncio.Queue(loop=self.loop)
        self.proxies = ProxyPool(proxy_queue)
        self.proxy_broker = Broker(
            queue=proxy_queue, loop=self.loop
        )

        self.kill_event = asyncio.Event(loop=self.loop)
        self.loop.add_signal_handler(signal.SIGINT, self.close)

    def load_config(self, path):
        with open(path, 'r') as f:
            self.config = json.load(f)

    def close(self):
        if self.items is not None:
            self.items.cancel()
        self.kill_event.set()
        self.proxy_broker.stop()
        self.pool.shutdown()

    async def scrape(self):
        futures = set()

        def enqueue(f):
            futures.remove(f)
            if not f.cancelled() and not f.exception():
                result = f.result()
                log.debug('Queuing cleaned item: %s', str(result))
                self.queue.put_nowait(result)

        self._setup()
        if self.using_proxies:
            futures.add(asyncio.ensure_future(self._update_proxies()))
        self.items = ItemMerger(*[gen() for gen in self.gens])
        async for item in self.items:
            future = self.loop.run_in_executor(self.pool, clean_item, item)
            future.add_done_callback(enqueue)
            futures.add(future)

        await asyncio.gather(*[s.close() for s in self.scrapers])
        for future in futures:
            future.cancel()

    def _setup(self):
        try:
            for source, auths in self.config.items():
                for auth, topics in auths.items():
                    use_proxies = topics.pop('use_proxies', False)
                    if use_proxies:
                        self.using_proxies = True
                    ath = auth.split('|') if auth else []
                    scraper = SCRAPERS[source](
                        *ath, proxy_pool=self.proxies if use_proxies else None
                    )
                    self.scrapers.append(scraper)
                    for topic, queries in topics.items():
                        for q in queries:
                            self.gens.append(
                                partial(scraper.stream, q, topic=topic)
                            )
        except Exception as e:
            raise ValueError('INCORRECT CONFIG - raised %s' % repr(e))

    async def _update_proxies(self):
        while not self.kill_event.is_set():
            await self.proxy_broker.find(
                strict=True,
                types=['HTTP', 'HTTPS'],
                judges=[
                    'http://httpbin.org/get?show_env',
                    'https://httpbin.org/get?show_env'
                ]
            )
