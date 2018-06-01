from collections import defaultdict
from functools import partial
import asyncio
import json
import logging
import signal
import threading

from proxybroker import Broker, ProxyPool

from .wrappers import ItemMerger, ItemProcessor, ItemSorter

log = logging.getLogger('veryscrape')


_mutex = threading.Lock()
_scrapers = {}
_classifying_scrapers = {}


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
            _classifying_scrapers[name] = scraper


def unregister(name):
    """
    Unregister scraper class registered with 'veryscrape.register'
    :param name: name of data source (e.g. 'twitter')
    """
    with _mutex:
        if name == '*':
            _scrapers.clear()
            _classifying_scrapers.clear()
        else:
            del _scrapers[name]
            if name in _classifying_scrapers:
                del _classifying_scrapers[name]


class VeryScrape:
    def __init__(self, q,
                 max_items_to_sort=0, max_item_age=None,
                 loop=None, n_cores=1):
        self.items = None
        self.loop = loop or asyncio.get_event_loop()
        self.max_age = max_item_age
        self.max_items = max_items_to_sort
        self.n_cores = n_cores
        self.queue = q
        self.topics_by_source = defaultdict(dict)
        self.using_proxies = False

        proxy_queue = asyncio.Queue(loop=self.loop)
        self.proxies = ProxyPool(proxy_queue)
        self.proxy_broker = Broker(
            queue=proxy_queue, loop=self.loop
        )

        self.kill_event = asyncio.Event(loop=self.loop)
        self.loop.add_signal_handler(signal.SIGINT, self.close)

    def close(self):
        self.kill_event.set()
        if self.items is not None:
            self.items.cancel()
        self.proxy_broker.stop()

    def create_all_scrapers_and_streams(self, config):
        scrapers = []
        streams = []
        for source, auth_topics in config.copy().items():
            for auth, metadata in auth_topics.items():

                args, kwargs = self._create_args_kwargs(auth, metadata)

                scraper, _streams = self._create_single_scraper_and_streams(
                    metadata, _scrapers[source], args, kwargs
                )

                self.topics_by_source[source].update(metadata)
                scrapers.append(scraper)
                streams.extend(_streams)

        return scrapers, streams

    async def scrape(self, config=None):
        if isinstance(config, str):
            with open(config, 'r') as f:
                config = json.load(f)
        else:
            config = config or {}

        try:
            scrapers, streams = self.create_all_scrapers_and_streams(config)
        except Exception as e:
            raise ValueError().with_traceback(e.__traceback__)

        self.items = ItemSorter(
            ItemProcessor(
                ItemMerger(*[
                    stream() for stream in streams
                ]), n_cores=self.n_cores, loop=self.loop
            ), max_items=self.max_items, max_age=self.max_age
        )

        # Update topics of ItemProcessor
        self.items.items.update_topics(**self.topics_by_source)

        # Start finding proxies if any scrapers use proxies
        if self.using_proxies:
            asyncio.ensure_future(self._update_proxies())

        async for item in self.items:
            await self.queue.put(item)

        await asyncio.gather(*[s.close() for s in scrapers])

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

    @staticmethod
    def _create_single_scraper_and_streams(topics, klass, args, kwargs):
        streams = []
        scraper = klass(*args, **kwargs)
        for topic, queries in topics.items():
            if klass in _classifying_scrapers.values():
                topic = '__classify__'
            streams.extend([
                partial(scraper.stream, q, topic=topic) for q in queries
            ])
        return scraper, streams

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
            await asyncio.sleep(180)  # default proxy-broker sleep cycle
