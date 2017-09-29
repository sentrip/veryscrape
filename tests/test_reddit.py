import unittest

from veryscrape import Producer, synchronous, get_auth
from veryscrape.extensions.reddit import Reddit


@synchronous
async def f():
    return await get_auth('reddit')


class RedditTest(unittest.TestCase):
    auths = f()
    topics = Producer.load_query_dictionary('subreddits')
    auth = {k: a for k, a in zip(sorted(topics.keys()), auths)}
    topic = next(iter(list(topics.keys())))
    q = 'all'

    def test_get_links(self):
        @synchronous
        async def run():
            reddit = Reddit(self.auth[self.topic])
            url = reddit.link_url.format(self.q, '')
            resp = await reddit.request('GET', url, oauth=2, return_json=True)
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
            await reddit.close()
        run()

    def test_get_comments(self):
        @synchronous
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
        @synchronous
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
