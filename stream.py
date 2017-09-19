import asyncio
import os
from multiprocessing import Process
from multiprocessing.connection import Listener
from scrapesync.base import load_query_dictionary, BASE_DIR
from scrapesync.extensions.twitter import twitter, load_authentications as lta
from scrapesync.extensions.reddit import reddit, load_authentications as lra


class StreamWorker(Process):
    def __init__(self, port=6000):
        super(StreamWorker, self).__init__()
        self.base_dir = BASE_DIR
        self.listener = Listener(('localhost', port), authkey=b'veryscrape')
        self.outgoing = None
        self.running = True
        # General
        self.topics = load_query_dictionary(os.path.join(BASE_DIR, 'lib', 'documents', 'query_topics1.txt'))
        # Twitter
        self.twitter_authentications = lta(BASE_DIR, self.topics)
        # Reddit
        self.reddit_authentications = lra(BASE_DIR, self.topics)
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
        self.outgoing = self.listener.accept()
        jobs = self.initialize_work()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.serve(jobs))
