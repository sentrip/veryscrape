from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from functools import partial
import asyncio
import json
import logging
import signal
import threading

from proxybroker import Broker, ProxyPool

from .items import ItemMerger
from .process import clean_item, classify_text

log = logging.getLogger('veryscrape')


_mutex = threading.Lock()
_scrapers = {}
_scrapers_that_classify = {}


def register(name, scraper, classify=False):
    """
    Register scraper class so it is created automatically
    from keys in VeryScrape.config when VeryScrape is run
    :param name: name of data source (e.g. 'twitter')
    :param scraper: scraper class
    :param classify: whether scraper needs to classify text topic afterwards
    """
    with _mutex:
        _scrapers[name] = scraper
        if classify:
            _scrapers_that_classify[name] = scraper


def unregister(name):
    """
    Unregister scraper class registered with 'veryscrape.register'
    :param name: name of data source (e.g. 'twitter')
    """
    with _mutex:
        if name == '*':
            _scrapers.clear()
            _scrapers_that_classify.clear()
        else:
            del _scrapers[name]
            if name in _scrapers_that_classify:
                del _scrapers_that_classify[name]


class VeryScrape:
    def __init__(self, q, config=None, loop=None, n_cores=1):
        self.queue = q
        self.loop = loop or asyncio.get_event_loop()
        self.pool = ProcessPoolExecutor(n_cores)
        self.gens = []
        self.scrapers = []
        self.items = None
        self.using_proxies = False
        self.topics_by_source = defaultdict(dict)

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
        self.kill_event.set()
        if self.items is not None:
            self.items.cancel()
        self.proxy_broker.stop()
        self.pool.shutdown()

    async def scrape(self):
        self._setup()
        self.items = ItemMerger(*[gen() for gen in self.gens])

        if self.using_proxies:
            asyncio.ensure_future(self._update_proxies())

        async for item in self.items:
            future = self.loop.run_in_executor(self.pool, clean_item, item)
            if item.topic == '__classify__':
                future.add_done_callback(self._classify_item)
            else:
                future.add_done_callback(self._enqueue_item)

        await asyncio.gather(*[s.close() for s in self.scrapers])

    def _setup(self):
        try:
            for source, auths in self.config.copy().items():
                for auth, metadata in auths.items():
                    args, kwargs = self._create_args_kwargs(auth, metadata)
                    self.topics_by_source[source].update(metadata)
                    self._create_scraper(
                        metadata, _scrapers[source], args, kwargs)

        except Exception as e:
            raise ValueError('INCORRECT CONFIG - raised %s' % repr(e))

    def _create_args_kwargs(self, auth, metadata):
        args = []
        if auth:
            args.extend(auth.split('|'))

        kwargs = {'proxy_pool': None}
        kwargs.update(metadata.pop('kwargs', {}))

        use_proxies = metadata.pop('use_proxies', False)
        if use_proxies:
            self.using_proxies = True
            kwargs.update(proxy_pool=self.proxies)

        return args, kwargs

    def _create_scraper(self, topics, klass, args, kwargs):
        scraper = klass(*args, **kwargs)
        self.scrapers.append(scraper)
        for topic, queries in topics.items():
            for q in queries:
                if klass in _scrapers_that_classify.values():
                    topic = '__classify__'
                self.gens.append(
                    partial(scraper.stream, q, topic=topic)
                )

    def _classify_item(self, future):
        if not future.cancelled() and not future.exception():
            item = future.result()
            f = self.pool.submit(
                classify_text,
                item.content, self.topics_by_source[item.source]
            )
            f.add_done_callback(partial(
                self._enqueue_classified_item, item=item))

    def _enqueue_classified_item(self, future, item=None):
        if not future.cancelled() and not future.exception():
            if item is not None:
                item.topic = future.result()
                log.debug('Queuing cleaned and classified item: %s', str(item))
                self.queue.put_nowait(item)

    def _enqueue_item(self, future):
        if not future.cancelled() and not future.exception():
            result = future.result()
            log.debug('Queuing cleaned item: %s', str(result))
            self.queue.put_nowait(result)

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
