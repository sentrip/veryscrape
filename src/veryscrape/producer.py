import json
from collections import defaultdict
from multiprocessing import Queue, Process

import aiohttp

from veryscrape.extensions import *
from veryscrape.preprocess import PreProcessor
from veryscrape.stream import download, get_topics, InfiniteScraper


class QueueFilter:
    def __init__(self, queue, interval=60):
        self.queue = queue
        self.interval = interval
        self.data = defaultdict(partial(defaultdict, list))
        self.start = time.time()
        self.companies = sorted(list(get_topics('topics').keys()))

    async def __anext__(self):
        averages = {k: {c: 0. for c in self.companies} for k in ['article', 'blog', 'reddit', 'twitter', 'stock']}
        while time.time() - self.start <= self.interval:
            item = await self.queue.get()
            self.data[item.source][item.topic].append(item.content)

        averages.update({t: {k: sum(s) / max(1, len(s)) for k, s in qs.items()} for t, qs in self.data.items()})
        for t, qs in self.data.items():
            for k in qs:
                self.data[t][k] = []

        self.start = time.time()
        return averages

    def __aiter__(self):
        return self


class Producer:
    item_server_url = 'http://192.168.1.53:9999'

    def __init__(self, send_every=60):
        self.send_every = send_every
        self.topics = get_topics('topics')
        self.subreddits = get_topics('subreddits')

        self.url_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()
        self.preprocess_queue = Queue()
        self.sentiment_queue = Queue()
        self.output_queue = asyncio.Queue()

    async def build_args_and_kwargs(self, concurrent_connections=10):
        args = []
        b = BaseScraper()
        twingly_auth = await b.get_api_data('twingly')
        twitter_auth = iter(await b.get_api_data('twitter'))
        reddit_auth = iter(await b.get_api_data('reddit'))

        finance = InfiniteScraper(Finance)
        twingly = InfiniteScraper(Twingly, twingly_auth)
        google = InfiniteScraper(Google)

        off, count = 0, 0

        for t, qs in self.topics.items():
            args.append((finance, [0, 60, t, t, self.output_queue], {'use_proxy': True}))

            twitter = InfiniteScraper(Twitter, next(twitter_auth))
            reddit = InfiniteScraper(Reddit, next(reddit_auth))

            for q, sr in zip(qs, self.subreddits[t]):
                args.append((twingly, [off, 900, q, t, self.url_queue], {}))
                args.append((google, [off, 900, q, t, self.url_queue], {'use_proxy': True}))
                args.append((twitter, [off, 0.25, q, t, self.result_queue], {'use_proxy': True}))
                args.append((reddit, [off, 15, q, t, self.result_queue], {}))

                count += 1
                if count % concurrent_connections == 0:
                    off += 5

        return args

    async def aggregate_queues(self):
        while True:
            if not self.sentiment_queue.empty():
                item = self.sentiment_queue.get_nowait()
                await self.output_queue.put(item)
            await asyncio.sleep(0)

    async def send_outputs(self):
        filt = QueueFilter(self.output_queue, interval=self.send_every)
        async with aiohttp.ClientSession() as sess:
            async for data in filt:
                async with sess.post(self.item_server_url, data=json.dumps(data)) as resp:
                    _ = await resp.text()

    @staticmethod
    def run_in_new_loop(args_list):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        new_loop = asyncio.get_event_loop()
        jobs = []
        for scraper, args, kwargs in args_list:
            jobs.append(scraper.scrape_forever(*args, **kwargs))
        new_loop.run_until_complete(asyncio.gather(*jobs))

    def run(self):
        main_loop = asyncio.get_event_loop()

        main_args_list = main_loop.run_until_complete(self.build_args_and_kwargs())
        for args_list in main_args_list:
            Process(target=self.run_in_new_loop, args=(args_list,)).start()

        preprocessor = PreProcessor(self.result_queue, self.preprocess_queue)
        main_loop.run_until_complete(asyncio.gather(download(self.url_queue, self.result_queue),
                                                    preprocessor.run(),
                                                    self.aggregate_queues(),
                                                    self.send_outputs(),
                                                    self.prnt()))

    async def prnt(self):
        while True:
            item = await self.result_queue.get()
            print(item)


if __name__ == '__main__':
    producer = Producer()
    producer.run()
