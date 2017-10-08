import json
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue
from threading import Thread

from veryscrape.preprocess import PreProcessor
from veryscrape.request import download, get_auth
from veryscrape.scrape import *

linux_path, windows_path = "/home/djordje/veryscrape/vs/documents", "C:/users/djordje/desktop/lib/documents"
BASE_DIR = linux_path if os.path.isdir(linux_path) else windows_path


def load_query_dictionary(file_name):
    """Loads query topics and corresponding queries from disk"""
    queries = {}
    with open(os.path.join(BASE_DIR, '%s.txt' % file_name), 'r') as f:
        lns = [l for i, l in enumerate(f.read().splitlines()) if any(l.startswith(q) for q in ['ATVI', "FB"])]
        for l in lns:
            x, y = l.split(':')
            queries[x] = y.split(',')
    return queries


class QueueFilter:
    def __init__(self, queue, interval=60):
        self.queue = queue
        self.interval = interval
        self.data = defaultdict(partial(defaultdict, list))
        self.start = time.time()
        self.companies = sorted(list(load_query_dictionary('query_topics').keys()))

    async def __anext__(self):
        averages = {k: {c: 0. for c in self.companies} for k in ['article', 'blog', 'reddit', 'twitter', 'stock']}
        while time.time() - self.start <= self.interval:
            if not self.queue.empty():
                item = self.queue.get_nowait()
                self.data[item.source][item.topic].append(item.content)
            await asyncio.sleep(0)
        self.start = time.time()
        averages.update({t: {k: sum(s) / max(1, len(s)) for k, s in qs.items()} for t, qs in self.data.items()})
        for t, qs in self.data.items():
            for k in qs:
                self.data[t][k] = []
        return averages

    def __aiter__(self):
        return self


class Producer:
    def __init__(self, send_every=60):
        self.send_every = send_every
        self.topics = load_query_dictionary('query_topics')
        self.subreddits = load_query_dictionary('subreddits')
        self.url_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()
        self.preprocess_queue = Queue()
        self.sentiment_queue = Queue()
        self.output_queue = asyncio.Queue()

    async def aggregate_queues(self):
        while True:
            if not self.sentiment_queue.empty():
                item = self.sentiment_queue.get_nowait()
                await self.output_queue.put(item)
            await asyncio.sleep(0)

    @staticmethod
    def run_in_new_loop(jobs):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        new_loop = asyncio.get_event_loop()
        new_loop.run_until_complete(asyncio.gather(*[f() for f in jobs]))

    async def get_jobs(self):
        jobs = [[], [], [], []]
        twingly_auth = (await get_auth('twingly'))[0][0]
        twitter_auth = iter(await get_auth('twitter'))
        reddit_auth = iter(await get_auth('reddit'))

        url_scrapers = [InfiniteScraper(Twingly, twingly_auth), InfiniteScraper(Google)]
        finance = InfiniteScraper(Finance)

        concurrent_connections = 10
        offset, count = 0, 0
        for t in self.topics:
            jobs[0].append(partial(finance.scrape_forever, 0, 60, t, t, self.output_queue, use_proxy=True))
            twitter = InfiniteScraper(Twitter, next(twitter_auth))
            reddit = InfiniteScraper(Reddit, next(reddit_auth))

            for q, sr in zip(self.topics[t], self.subreddits[t]):
                    jobs[1].append(partial(twitter.scrape_forever, offset, 0.25, q, t, self.result_queue, use_proxy=True))
                    jobs[2].append(partial(reddit.scrape_forever, offset, 15, sr, t, self.result_queue))
                    for scraper, up in zip(url_scrapers, [False, True]):
                        jobs[3].append(partial(scraper.scrape_forever, offset, 900, q, t, self.url_queue, use_proxy=up))

                    count += 1
                    if count % concurrent_connections == 0:
                        offset += 5
        return jobs

    async def send_outputs(self):
        filt = QueueFilter(self.output_queue, interval=self.send_every)
        async with aiohttp.ClientSession() as sess:
            async for data in filt:
                async with sess.post('http://192.168.1.53:9999', data=json.dumps(data)) as resp:
                    _ = await resp.text()

    async def prnt(self):
        while True:
            item = await self.result_queue.get()
            print(item)

    def run(self):
        pool = ProcessPoolExecutor(2)
        preprocessor = PreProcessor(self.result_queue, self.preprocess_queue, pool)
        main_loop = asyncio.get_event_loop()
        jobs = main_loop.run_until_complete(self.get_jobs())
        for set_of_jobs in jobs:
            Thread(target=self.run_in_new_loop, args=(set_of_jobs,)).start()
        main_loop.run_until_complete(asyncio.gather(preprocessor.run(), download(self.url_queue, self.result_queue),
                                                    self.aggregate_queues(), self.send_outputs(), self.prnt()))


if __name__ == '__main__':
    producer = Producer()
    producer.run()
