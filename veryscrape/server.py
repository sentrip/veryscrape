import asyncio
import concurrent.futures
import os
import sys

from aioserverplus import serve, SocketHandler

from preprocess import PreProcessor
from scrapers import Reddit, Twitter


def load_auths(path):
    auths = {}
    for name in ['twitter', 'reddit']:
        with open(os.path.join(path, 'api', name+'.txt'), 'r') as f:
            data = f.read().splitlines()[:2]
            auths[name] = [i.split('|') for i in data]
    return auths


def load_dict(path, name):
    topics = {}
    with open(os.path.join(path, name+'.txt'), 'r') as f:
        data = f.read().splitlines()
    for i in data:
        topic, queries = i.split(':')
        topics[topic] = queries.split(',')
    return topics


def load_args_from_path(path):
    sources = ['all', 'reddit', 'twitter', 'article']
    topics = load_dict(path, 'topics')
    subreddits = load_dict(path, 'subreddits')
    return load_auths(path), sources, topics, subreddits


class ScrapeHandler(SocketHandler):
    max_queue_size = 100000

    def __init__(self, auths, sources, topics, subreddits, *args, **kwargs):
        super(ScrapeHandler, self).__init__(*args, **kwargs)
        self.auths = auths
        self.sources = sources
        self.topics = topics
        self.subreddits = subreddits
        self.raw_queue, self.data_queue, self.url_queue = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        self.queues = {k: {c: asyncio.Queue() for c in self.topics} for k in self.sources}

    async def merge_queues(self):
        while True:
            d = await self.data_queue.get()
            await self.queues[d.source][d.topic].put(d)
            await self.queues['all'][d.topic].put(d)

    async def load_sources(self):
        sources = []
        for topic in self.topics:
            for source in self.sources:
                sources.append((source, topic))
        return sources

    async def get_data(self, source):
        src, topic = source
        while self.queues[src][topic].qsize() > self.max_queue_size:
            _ = await self.queues[src][topic].get()
        item = await self.queues[src][topic].get()
        return {'source': item.source, 'topic': topic, 'content': item.content}

    def parse_sources(self, request):
        src = request.params['source'] or 'all'
        topics = request.params['topics'].split(',')
        return [(src, topic) for topic in topics]


async def backend_init(handler):
    handler.jobs = []
    #google = Google(output_queue=handler.url_queue)
    for topic, reddit_auth, twitter_auth in zip(handler.topics.keys(), handler.auths['reddit'], handler.auths['twitter']):
        reddit = Reddit(reddit_auth, output_queue=handler.raw_queue)
        twitter = Twitter(twitter_auth, output_queue=handler.raw_queue)
        for subreddit in handler.subreddits[topic]:
            handler.jobs.append(reddit.scrape(subreddit, topic))
        for query in handler.topics[topic]:
            #handler.jobs.append(google.scrape(query, topic))
            handler.jobs.append(twitter.scrape(query, topic))


async def backend(handler):
    pool = concurrent.futures.ThreadPoolExecutor(4)
    handler.preprocessor = PreProcessor(handler.raw_queue, handler.data_queue)
    await asyncio.gather(
        *handler.jobs,
        handler.preprocessor.run(pool),
        handler.merge_queues(),
        #download(handler.url_queue, handler.raw_queue)
    )


async def backend_shutdown(handler):
    handler.preprocessor.running = False


if __name__ == "__main__":
    try:
        if not sys.argv[1].startswith('-'):
            a = load_args_from_path(sys.argv[1])
        else:
            raise IndexError
    except IndexError:
        raise FileNotFoundError('Need to specify a source directory for topic data')
    serve(log_level="INFO", log_modules=['aioserverplus', 'veryscrape'],
          handler=ScrapeHandler, handler_args=a,
          backend=backend, backend_init=backend_init, backend_shutdown=backend_shutdown)
