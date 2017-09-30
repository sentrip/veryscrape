import asyncio
import random
import time
import unittest
from multiprocessing import Queue

from veryscrape import Item, load_query_dictionary, queue_filter, synchronous, retry


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
    async def test_retry(self):
        random.seed(1875807)
        s_times, f_times = [], []

        @retry(3, initial_wait=0.01, test=True)
        async def success():
            s_times.append(time.time() - astart)
            if random.random() < 0.5:
                raise Exception
            else:
                return 'success'

        @retry(3, initial_wait=0.01, test=True)
        async def fail():
            f_times.append(time.time() - astart)
            raise Exception
        astart = time.time()
        s = await success()
        astart = time.time()
        failed = False
        try:
            f = await fail()
        except TimeoutError:
            failed = True
        assert s == 'success', 'Unsuccessfully returned'
        assert failed, 'Did not fail successfully'
        diff = 0.01
        for i in range(len(s_times)-1):
            sd = round(s_times[i + 1] - s_times[i], 2)
            fd = round(f_times[i + 1] - s_times[i], 2)
            assert sd == diff, 'Success time difference not correct, {}'.format(sd)
            assert fd == diff, 'Fail time difference not correct, {}'.format(fd)
            diff *= 2

    @synchronous
    async def test_async_run_forever(self):
        pass

    @synchronous
    async def test_get_auth(self):
        pass

    def test_synchronous(self):
        @synchronous
        async def t():
            await asyncio.sleep(0)
            return True
        assert t(), 'Async function did not run correctly'

    def test_load_query_dictionary(self):
        d = load_query_dictionary('query_topics')
        assert len(d) == 110, 'Dictionary length incorrect'
        assert all(q for _, q in d.items())
        d = load_query_dictionary('subreddits')
        assert len(d) == 110, 'Dictionary length incorrect'
        assert all(q for _, q in d.items())

if __name__ == '__main__':
    unittest.main()
