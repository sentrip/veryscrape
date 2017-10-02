import unittest
import asyncio
import json
from collections import namedtuple
from io import BytesIO
from veryscrape import load_query_dictionary, synchronous, get_auth, Item
from veryscrape.extensions import *
from veryscrape.extensions.twitter import ReadBuffer


class TestFinance(unittest.TestCase):
    topics = load_query_dictionary('query_topics')
    topic = next(iter(list(topics.keys())))

    @synchronous
    async def setUp(self):
        self.url_queue = asyncio.Queue()
        self.client = Finance(self.url_queue)

    @synchronous
    async def tearDown(self):
        await self.client.close()
        while not self.url_queue.empty():
            _ = self.url_queue.get_nowait()

    @synchronous
    async def test_finance_single_request_no_proxy(self):
        async with await self.client.get(self.client.finance_search_path, params={'q': self.topic}) as resp:
            assert resp.status == 200, 'Request retuned {} error'.format(resp.status)


    @synchronous
    async def test_finance_stream_no_proxy(self):
        await self.client.finance_stream(self.topic, duration=0.1, send_every=0)
        assert self.url_queue.get_nowait(), "Url queue empty!"

    # @synchronous
    # async def test_finance_single_request_proxy(self):
    #     resp = await self.client.get(self.client.finance_search_path, params={'q': self.topic},
    #                                  stream=True, use_proxy={'speed': 50, 'https': 1})
    #     assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
    #     resp.close()
    #
    # @synchronous
    # async def test_finance_stream_proxy(self):
    #     await self.client.finance_stream(self.topic, duration=0.1, send_every=0, use_proxy=True)
    #     assert self.url_queue.get_nowait(), "Url queue empty!"


class TestGoogle(unittest.TestCase):
    topics = load_query_dictionary('query_topics')
    topic = next(iter(list(topics.keys())))
    q = 'tesla'

    @synchronous
    async def setUp(self):
        self.url_queue = asyncio.Queue()
        self.client = Google(self.url_queue)

    @synchronous
    async def tearDown(self):
        await self.client.close()
        while not self.url_queue.empty():
            _ = self.url_queue.get_nowait()

    @synchronous
    async def test_google_single_request_no_proxy(self):
        async with await self.client.get(self.client.article_search_path.format(self.q, self.q)) as resp:
            assert resp.status == 200, 'Request retuned {} error'.format(resp.status)


    @synchronous
    async def test_google_stream_no_proxy(self):
        await self.client.article_stream(self.q, self.topic, duration=0.1)
        assert self.url_queue.get_nowait(), "Url queue empty!"

    # @synchronous
    # async def test_google_single_request_proxy(self):
    #     resp = await self.client.get(self.client.article_search_path.format(self.q, self.q),
    #                                  stream=True, use_proxy={'speed': 50, 'https': 1})
    #     assert resp.status == 200, 'Request retuned {} error'.format(resp.status)
    #     resp.close()
    #
    # @synchronous
    # async def test_google_stream_proxy(self):
    #     await self.client.article_stream(self.q, self.topic, duration=0.1, use_proxy=True)
    #     assert self.url_queue.get_nowait(), "Url queue empty!"


@synchronous
async def fr():
    return await get_auth('reddit')


class RedditTest(unittest.TestCase):
    auths = fr()
    topics = load_query_dictionary('subreddits')
    auth = {k: a for k, a in zip(sorted(topics.keys()), auths)}
    topic = next(iter(list(topics.keys())))
    q = 'all'

    @synchronous
    async def setUp(self):
        self.qu = asyncio.Queue()
        self.reddit = Reddit(self.auth[self.topic], self.qu)

    @synchronous
    async def tearDown(self):
        while not self.qu.empty():
            _ = self.qu.get_nowait()
        await self.reddit.close()

    @synchronous
    async def test_get_links(self):
        url = self.reddit.link_url.format(self.q, '')
        async with await self.reddit.get(url, oauth=2) as raw:
            resp = await raw.json()
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'

    @synchronous
    async def test_get_comments(self):
        url = self.reddit.link_url.format(self.q, '')
        async with await self.reddit.get(url, oauth=2) as raw:
            resp = await raw.json()
            assert resp, 'Empty json'
            assert resp['data'], 'Invalid response'
        url = self.reddit.comment_url.format(self.q, resp['data']['children'][-1]['data']['id'], '')
        async with await self.reddit.get(url, oauth=2) as raw:
            resp = await raw.json()
            assert resp, 'Empty json'
            assert resp[0]['data'], 'Invalid response'
            assert isinstance(resp[1]['data']['children'], list), 'Comments incorrectly returned'

    @synchronous
    async def test_send_comments(self):
        try:
            with open('data/comments_json.txt', 'r') as f:
                cmnts = eval(f.read())
        except FileNotFoundError:
            with open('tests_veryscrape/data/comments_json.txt', 'r') as f:
                cmnts = eval(f.read())
        await self.reddit.send_comments(cmnts, self.topic)
        assert self.qu.get_nowait(), 'Queue is empty!'


class TestTwitter(unittest.TestCase):

    @synchronous
    async def setUp(self):
        self.topics = load_query_dictionary('query_topics')
        self.auth = {k: a for k, a in zip(sorted(self.topics.keys()), await get_auth('twitter'))}
        self.queue = asyncio.Queue()
        self.twitter = Twitter(self.auth['FB'], self.queue)

    @synchronous
    async def tearDown(self):
        await self.twitter.close()
        while not self.queue.empty():
            _ = self.queue.get_nowait()

    @synchronous
    async def test_readbuffer_small_stream(self):
        tweets = [{'status': 'This tweet'}, {'status': 'This tweet'}]
        mock_stream_content = bytes('{}\r\n\r{}\r\n'.format(*list(map(json.dumps, tweets))), encoding='utf-8')
        mock_stream = BytesIO(mock_stream_content)
        mock_stream.headers = {}
        buf = ReadBuffer(mock_stream)
        async for item in buf:
            assert item == 'This item', 'Did not return same item!'

    @synchronous
    async def test_readbuffer_large_stream(self):
        tweets = [{'status': 'This tweet'}] * 10000
        ms = namedtuple('MockStream', ['content', 'headers'])
        s = '\r\n\r'.join(['{}']*10000) + '\r\n'
        mock_stream_content = bytes(s.format(*list(map(json.dumps, tweets))), encoding='utf-8')
        mock_stream = ms(BytesIO(mock_stream_content), {})
        buf = ReadBuffer(mock_stream)
        async for item in buf:
            assert item == 'This item', 'Did not return same item!'

    @synchronous
    async def test_twitter_acquire_stream(self):
        params = {'language': 'en', 'track': 'apple'}
        async with await self.twitter.post('statuses/filter.json', {}, params=params, oauth=1) as raw:
            assert raw.status == 200, 'Request failed, returned error code {}'.format(raw.status)

    @synchronous
    async def test_twitter_filter_stream_no_proxy(self):
        await self.twitter.filter_stream('apple', 'AAPL', duration=1, use_proxy=False)

    # @synchronous
    # def test_twitter_filter_stream_proxy(self):
    #     twitter = Twitter(self.auth['AAPL'])
    #     await twitter.filter_stream('apple', 'AAPL', duration=1, use_proxy=True)
    #     await twitter.close()


@synchronous
async def ft():
    return await get_auth('twingly')


class TestTwingly(unittest.TestCase):
    topics = load_query_dictionary('query_topics')
    topic = next(iter(list(topics.keys())))
    q = 'tesla'
    auth = ft()[0][0]

    @synchronous
    async def setUp(self):
        self.url_queue = asyncio.Queue()
        self.client = Twingly(self.auth, self.url_queue)

    @synchronous
    async def tearDown(self):
        await self.client.close()
        while not self.url_queue.empty():
            _ = self.url_queue.get_nowait()

    @synchronous
    async def test_twingly_single_request(self):
        params = {'apiKey': self.client.client, 'q': self.client.build_query(self.q)}
        async with await self.client.get(self.client.blog_search_path, params=params) as raw:
            assert raw.status != 401, 'Twingly search unauthorized'
            assert raw.status == 200, 'Request retuned {} error'.format(raw.status)


    @synchronous
    async def test_twingly_stream(self):
        await self.client.blog_stream(self.q, self.topic, duration=0)
        assert self.url_queue.get_nowait(), "Url queue empty!"


class TestDownload(unittest.TestCase):
    url_queue = asyncio.Queue()
    result_queue = asyncio.Queue()

    def setUp(self):
        self.topics = load_query_dictionary('query_topics')

    @synchronous
    async def test_download_small_url_set(self):
        urls = ['http://fortune.com/2017/09/29/apple-mac-update-security-sierra-firmware/',
                'https://techcrunch.com/2017/09/29/apple-quietly-acquires-computer-vision-startup-regaind/',
                'http://www.businessinsider.com/apple-considers-ditching-intel-making-own-chip-for-mac-laptops-report-2017-9']
        down = Download(self.url_queue, self.result_queue)
        for u in urls:
            await self.url_queue.put(Item(u, 'AAPL', 'article'))
        await down.stream(duration=1)
        assert self.url_queue.empty(), 'Url queue was not purged'
        res = self.result_queue.get_nowait()
        assert res, 'Result queue is empty'
        assert res.content.startswith('<'), 'Incorrect response from download'
        await down.close()


if __name__ == '__main__':
    unittest.main()
