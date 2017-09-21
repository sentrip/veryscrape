from multiprocessing import Queue
from threading import Thread

from base import Producer
from extensions.reddit import reddit
from extensions.search import search
from extensions.twitter import twitter


class StreamWorker(Producer):
    def __init__(self, port=6000, use_processes=True):
        super(StreamWorker, self).__init__(port, use_processes)
        self.url_queue = Queue()
        # Twitter
        self.twitter_authentications = self.load_authentications('twitter.txt', self.topics)
        # Reddit
        self.reddit_authentications = self.load_authentications('reddit.txt', self.topics)
        self.subreddits = self.load_query_dictionary('subreddits1.txt')
        # Rate limits
        self.reddit_rate_limit = {k: 60 for k in self.topics}

    def initialize_work(self):
        Thread(target=search, args=(self, )).start()
        jobs = [[], []]
        for topic in self.topics:
            for query in self.subreddits[topic]:
                jobs[0].append(reddit(self, topic, query))
            for query in self.topics[topic]:
                jobs[1].append(twitter(self, topic, query))
        return jobs
