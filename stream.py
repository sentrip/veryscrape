from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
from threading import Thread

import requests
from fake_useragent import UserAgent

from base import Producer, Item
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

    def single_download(self, sess, item):
        try:
            response = sess.get(item.content, headers={'user-agent': UserAgent().random})
            self.result_queue.put(Item(str(response.content), item.topic, item.source))
        except:
            pass

    def download(self):
        """Async downloading of html text from article urls"""
        sess = requests.Session()
        pool = ThreadPoolExecutor(100)
        while self.running:
            if not self.url_queue.empty():
                item = self.url_queue.get_nowait()
                pool.submit(self.single_download, sess, item)
        sess.close()

    def initialize_work(self):
        Thread(target=self.download).start()
        jobs = [[], [], []]
        for topic in self.topics:
            for query in self.subreddits[topic]:
                jobs[0].append(reddit(self, topic, query))
            for query in self.topics[topic]:
                jobs[1].append(twitter(self, topic, query))
                jobs[2].append(search(self, topic, query))
        return jobs
