import random
import unittest
from multiprocessing import Queue

from veryscrape import Item, load_query_dictionary, queue_filter, synchronous


class TestBASE(unittest.TestCase):
    queue = Queue()
    topics = load_query_dictionary('query_topics')
    for t in topics:
        for k in ['twitter', 'reddit', 'article', 'blog']:
            for _ in range(2):
                queue.put(Item(random.random(), t, k))

    def test_queue_filter(self):
        filt = queue_filter(self.queue, interval=0.25)
        d = next(filt)
        assert len(d) == len(self.topics), 'Returned dictionary of incorrect length, {}'.format(len(d))
        assert set(list(d.keys()) + list(self.topics.keys())) == set(self.topics.keys()), 'Incorrect keys returned'

    @synchronous
    def test_retry(self):
        pass

    @synchronous
    def test_async_run_forever(self):
        pass

    @synchronous
    async def test_get_auth(self):
        pass

    def test_synchronous(self):
        pass

    def test_load_query_dictionary(self):
        pass

if __name__ == '__main__':
    unittest.main()
