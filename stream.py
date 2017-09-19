import os
from multiprocessing import Queue

from base import BASE_DIR, load_authentications, load_query_dictionary, download, Producer
from extensions.google import google
from extensions.reddit import reddit
from extensions.twingly import twingly
from extensions.twitter import twitter


class StreamWorker(Producer):
    def __init__(self, port=6000):
        super(StreamWorker, self).__init__(port)
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

    def initialize_work(self):
        jobs = [download(self)]
        for topic in self.topics:
            for query in self.topics[topic]:
                jobs.append(twitter(self, topic, query))
                jobs.append(twingly(self, topic, query))
                jobs.append(google(self, topic, query))
            for query in self.subreddits[topic]:
                jobs.append(reddit(self, topic, query))
        return jobs
