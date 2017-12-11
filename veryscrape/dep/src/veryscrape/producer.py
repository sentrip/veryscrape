import json
from collections import defaultdict
from multiprocessing import Queue
from threading import Thread

import aiohttp

from veryscrape.extensions import *
from veryscrape.preprocess import PreProcessor
from veryscrape.sentiment import Sentiment
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
        args = [[], [], [], [], []]
        b = BaseScraper()
        twingly_auth = await b.get_api_data('twingly')
        twitter_auth = iter(await b.get_api_data('twitter'))
        reddit_auth = iter(await b.get_api_data('reddit'))

        finance = InfiniteScraper(Finance)
        twingly = InfiniteScraper(Twingly, twingly_auth)
        google = InfiniteScraper(Google)

        off, count = 0, 0

        for t, qs in list(self.topics.items())[:5]:
            args[0].append((finance, [0, 60, t, t, self.output_queue], {'use_proxy': True}))

            twitter = InfiniteScraper(Twitter, next(twitter_auth))
            reddit = InfiniteScraper(Reddit, next(reddit_auth))

            for q, sr in zip(qs, self.subreddits[t]):
                args[1].append((twingly, [off, 900, q, t, self.url_queue], {}))
                args[2].append((google, [off, 900, q, t, self.url_queue], {'use_proxy': True}))
                args[3].append((twitter, [off, 0.25, q, t, self.result_queue], {'use_proxy': True}))
                args[4].append((reddit, [off, 15, sr, t, self.result_queue], {}))

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

    def run(self):
        main_loop = asyncio.get_event_loop()

        main_args_list = main_loop.run_until_complete(self.build_args_and_kwargs())
        jobs = []
        for args_list in main_args_list:
            for set_of_args in args_list:
                jobs.append(set_of_args[0].scrape_forever(*set_of_args[1], **set_of_args[2]))

        PreProcessor(self.result_queue, self.preprocess_queue).start()
        Sentiment(self.preprocess_queue, self.sentiment_queue).start()
        Thread(target=self.prnt).start()

        main_loop.run_until_complete(asyncio.gather(*jobs,
                                                    download(self.url_queue, self.result_queue),
                                                    self.aggregate_queues()
                                                    # self.send_outputs()
                                                    ))

    def prnt(self):
        while True:
            if not self.output_queue.empty():
                item = self.output_queue.get_nowait()
                print(item)
            time.sleep(0.01)


if __name__ == '__main__':
    producer = Producer()
    producer.run()
