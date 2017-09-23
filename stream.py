from multiprocessing import Queue

from base import Producer
from extensions.reddit import CommentStream
from extensions.twitter import TweetStream


class StreamWorker(Producer):
    def __init__(self, port=6000, use_processes=True):
        super(StreamWorker, self).__init__(port, use_processes)
        self.url_queue = Queue()
        # Twitter
        self.twitter_auths = self.load_authentications('twitter.txt', self.topics)
        # Reddit
        self.reddit_auths = self.load_authentications('reddit.txt', self.topics)
        self.subreddits = self.load_query_dictionary('subreddits.txt')
        # Rate limits
        self.reddit_rate_limit = {k: 60 for k in self.topics}

    def initialize_work(self):
        #NewsStream(self).start()
        jobs = [[], []]
        for topic, qs in self.topics.items():
            jobs[0].append(CommentStream(self.reddit_auths[topic], topic, self.subreddits[topic], self.result_queue).stream())
            jobs[1].append(TweetStream(self.twitter_auths[topic], topic, qs, self.result_queue).stream())
        return jobs
