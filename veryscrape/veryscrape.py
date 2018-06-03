from collections import defaultdict
from functools import partial
from multiprocessing import cpu_count
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
    """
    Many API, much data, VeryScrape!

    :param q: Queue to output data gathered from scraping
    :param loop: Event loop to run the scraping
    """
    def __init__(self, q, loop=None):
        # declaring items in __init__ allows the items to
        # be cancelled from the close method of this class
        self.items = None
        self.loop = loop or asyncio.get_event_loop()
        self.queue = q
        self.using_proxies = False

        proxy_queue = asyncio.Queue(loop=self.loop)
        self.proxies = ProxyPool(proxy_queue)
        self.proxy_broker = Broker(
            queue=proxy_queue, loop=self.loop
        )

        self.kill_event = asyncio.Event(loop=self.loop)
        self.loop.add_signal_handler(signal.SIGINT, self.close)

    async def scrape(self, config, *, n_cores=1, max_items=0, max_age=None):
        """
        Scrape, process and organize data on the web based on a scrape config
        :param config: dict: scrape configuration
        This is a map of scrape sources to data gathering information.
        The basic scheme is as follows (see examples for real example):
        {
            "source1":
                {
                    "first_authentication|split|by|pipe":
                        {
                            "topic1": ["query1", "query2"],
                            "topic2": ["query3", "query4"]
                        },
                    "second_authentication|split|by|pipe":
                        {
                            "topic3": ["query5", "query6"]
                        }
                },

            "source2": ...
        }
        :param n_cores: number of cores to use for processing data
        Set to 0 to use all available cores. Set to -1 to disable processing.
        :param max_items:
        :param max_age:
        """
        if isinstance(config, str):
            with open(config) as f:
                config = json.load(f)

        assert isinstance(config, dict), \
            'Configuration must be a dict or a path to a json config file'

        try:
            scrapers, streams, topics = \
                self.create_all_scrapers_and_streams(config)
        except Exception as e:
            raise ValueError().with_traceback(e.__traceback__)

        self.items = ItemMerger(*[stream() for stream in streams])

        if n_cores > -1:
            self.items = ItemProcessor(self.items,
                                       # one core is needed to run event loop
                                       n_cores=n_cores or cpu_count() - 1,
                                       loop=self.loop)
            # Update topics of ItemProcessor for classifying
            self.items.update_topics(**topics)

        if max_items > 0 or max_age is not None:
            self.items = ItemSorter(self.items,
                                    max_items=max_items, max_age=max_age)

        # Start finding proxies if any scrapers use proxies
        if self.using_proxies:
            asyncio.ensure_future(self._update_proxies())

        async for item in self.items:
            await self.queue.put(item)

        await asyncio.gather(*[s.close() for s in scrapers])

    def close(self):
        self.kill_event.set()
        if self.items is not None:
            self.items.cancel()
        self.proxy_broker.stop()

    def create_all_scrapers_and_streams(self, config):
        """
        Creates all scrapers and stream functions associated with a config
        A scraper is a class inheriting from scrape.Scraper
        A stream function is a function returning an items.ItemGenerator

        :param config: scrape configuration
        :return: list of scrapers, list of stream functions
        """
        scrapers = []
        streams = []
        topics_by_source = defaultdict(dict)

        for source, auth_topics in config.copy().items():
            for auth, metadata in auth_topics.items():

                args, kwargs = self._create_args_kwargs(auth, metadata)

                scraper, _streams = self._create_single_scraper_and_streams(
                    metadata, _scrapers[source], args, kwargs
                )

                topics_by_source[source].update(metadata)
                scrapers.append(scraper)
                streams.extend(_streams)

        return scrapers, streams, topics_by_source

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
            for q in queries:
                streams.append(partial(scraper.stream, q, topic=topic))

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
            # default proxy-broker sleep cycle for continuous find
            await asyncio.sleep(180)  # pragma: nocover
