import asyncio
import unittest
from functools import wraps

from veryscrape import Producer, synchronous, get_auth
from veryscrape.extensions.reddit import Reddit


def run_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        l = asyncio.get_event_loop()
        return l.run_until_complete(f(*args, **kwargs))
    return wrapper


class RedditTest(unittest.TestCase):

    def setUp(self):
        @synchronous
        async def f():
            return await get_auth('reddit')
        auth = f()
        self.topics = Producer.load_query_dictionary('subreddits')
        self.auth = {k: a for k, a in zip(sorted(self.topics.keys()), auth)}
        self.topic = next(iter(list(self.topics.keys())))
        self.q = self.topics[self.topic][0]

    def test_get_links(self):
        @run_async
        async def run():
            reddit = Reddit(self.auth[self.topic])
            url = reddit.link_url.format(self.q, '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
            await reddit.close()
        run()

    def test_get_comments(self):
        @run_async
        async def run():
            reddit = Reddit(self.auth[self.topic])
            url = reddit.link_url.format(self.q, '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
            url = reddit.comment_url.format(self.q, resp['data']['children'][-1]['data']['id'], '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp[0]['data'], 'Invalid response'
            assert isinstance(resp[1]['data']['children'], list), 'Comments incorrectly returned'
            await reddit.close()
        run()

    def test_send_comments(self):
        @run_async
        async def run():
            reddit = Reddit(self.auth[self.topic])
            url = reddit.link_url.format(self.q, '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
            url = reddit.comment_url.format(self.q, resp['data']['children'][-1]['data']['id'], '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp[0]['data'], 'Invalid response'
            assert isinstance(resp[1]['data']['children'], list), 'Comments incorrectly returned'
            await reddit.send_comments(resp[1]['data']['children'], self.topic)
            await reddit.close()
        run()

if __name__ == '__main__':
    unittest.main()
