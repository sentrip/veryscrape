import asyncio
import os
from multiprocessing import Process, Queue
from multiprocessing.connection import Listener

from base import load_authentications, load_query_dictionary, BASE_DIR
from extensions.reddit import reddit
from extensions.twitter import twitter
from extensions.twingly import twingly


class StreamWorker(Process):
    def __init__(self, port=6000):
        super(StreamWorker, self).__init__()
        self.base_dir = BASE_DIR
        self.port = port
        self.outgoing = None
        self.running = True
        # General
        self.topics = load_query_dictionary(os.path.join(BASE_DIR, 'lib', 'documents', 'query_topics1.txt'))
        self.url_queue = Queue()
        # Twitter
        self.twitter_authentications = load_authentications(os.path.join(BASE_DIR, 'lib', 'api', 'twitter.txt'),
                                                            self.topics)
        # Reddit
        self.reddit_authentications = load_authentications(os.path.join(BASE_DIR, 'lib', 'api', 'reddit.txt'),
                                                           self.topics)
        self.subreddits = load_query_dictionary(os.path.join(BASE_DIR, 'lib', 'documents', 'subreddits1.txt'))
        # Rate limits
        self.reddit_rate_limit = {k: 60 for k in self.topics}
        self.google_rate_limit = 60
        self.twingly_rate_limit = 60

    def initialize_work(self):
        jobs = []
        for topic in self.topics:
            for query in self.topics[topic]:
                jobs.append(twitter(self, topic, query))
                jobs.append(twingly(self, topic, query))
            for query in self.subreddits[topic]:
                jobs.append(reddit(self, topic, query))
        return jobs

    async def send(self, item):
        self.outgoing.send(item)
        await asyncio.sleep(0)

    async def serve(self, jobs):
        while self.running:
            await asyncio.gather(*[asyncio.ensure_future(j) for j in jobs])

    def run(self):
        listener = Listener(('localhost', self.port), authkey=b'veryscrape')
        self.outgoing = listener.accept()
        jobs = self.initialize_work()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.serve(jobs))
