from multiprocessing import Queue

from src.base import Producer
from src.extensions.reddit import CommentStream
from src.extensions.search import SearchStream, DownloadStream
from src.extensions.twitter import TweetStream


class StreamWorker(Producer):
    def __init__(self, port=6000, use_processes=True):
        super(StreamWorker, self).__init__(port, use_processes)
        self.url_queue = Queue()
        self.topics = self.load_query_dictionary('query_topics.txt')
        self.subreddits = self.load_query_dictionary('subreddits.txt')
        self.reddit_rate_limit = {k: 60 for k in self.subreddits}
        self.auths = {
            'reddit': self.load_authentications('reddit.txt', self.topics),
            'twitter': self.load_authentications('twitter.txt', self.topics)
        }
        
    def initialize_work(self):
        jobs = [[], [], []] + [[DownloadStream(self.url_queue, self.result_queue).stream()]]
        for t, qs in self.topics.items():
            jobs[0].append(CommentStream(self.auths['reddit'][t], t, self.subreddits[t], self.result_queue).stream())
            jobs[1].append(TweetStream(self.auths['twitter'][t],  t, qs, self.result_queue).stream())
            jobs[2].append(SearchStream('article', t, qs, self.url_queue).stream())
            jobs[2].append(SearchStream('blog',    t, qs, self.url_queue).stream())
        return jobs
