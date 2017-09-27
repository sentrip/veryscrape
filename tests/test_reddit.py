import asyncio
import unittest
from functools import wraps
from veryscrape import Producer
from veryscrape.extensions.reddit import Reddit


def run_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        l = asyncio.get_event_loop()
        return l.run_until_complete(f(*args, **kwargs))
    return wrapper


class RedditTest(unittest.TestCase):
    last_link = None
    last_comment = None

    def setUp(self):
        self.topics = Producer.load_query_dictionary('subreddits.txt')
        self.auth = Producer.load_authentications('reddit.txt')
        self.topic = next(iter(list(self.topics.keys())))
        self.q = self.topics[self.topic][0]

    def test_get_links(self):
        @run_async
        async def run():
            reddit = Reddit(self.auth[self.topic])
            links = await reddit.get_links(self.q)
            self.last_link = links.popitem()[0]
            assert len(links) > 0 and len(self.last_link) == 6, 'Did not retrieve any links'
            await reddit.close()
        run()

    def test_get_zcomments(self):
        @run_async
        async def run():
            reddit = Reddit(self.auth[self.topic])
            if self.last_link is None:
                links = await reddit.get_links(self.q)
                self.last_link = links.popitem()[0]
            comments = await reddit.get_comments(self.q, self.last_link)
            self.last_comment = comments.pop()['data']['body_html']
            assert len(comments) > 0, 'Did not retrieve any comments'
            assert self.last_comment.startswith('<'), 'Incorrect comment response format'
            await reddit.close()
        run()

    def test_send_comments(self):
        @run_async
        async def run():
            reddit = Reddit(self.auth[self.topic])
            if self.last_comment is None:
                links = await reddit.get_links(self.q)
                self.last_link = links.popitem()[0]
                comments = await reddit.get_comments(self.q, self.last_link)
                self.last_comment = comments.pop()
            await reddit.send_comments([self.last_comment], self.topic)
            await reddit.close()
        run()

if __name__ == '__main__':
    unittest.main()
