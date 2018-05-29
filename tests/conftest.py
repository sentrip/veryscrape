import os, sys
sys.path.append(os.path.abspath('../veryscrape'))
from contextlib import contextmanager
from collections import defaultdict, deque
from functools import partial
import asyncio
import json
import pytest
import time
import aiohttp
import veryscrape.session
from veryscrape.items import ItemGenerator
from veryscrape.scrape import Scraper, HTMLScraper

DATA_PATH = 'tests/data/'

TWEETS = []
with open(DATA_PATH + 'tweets.txt', encoding='utf-8', errors='replace') as f:
    for i, ln in enumerate(f):
        TWEETS.append(ln.strip('\n'))
COMMENTS = []
with open(DATA_PATH + 'reddit_comments.txt', encoding='utf-8', errors='replace') as f:
    for ln in f:
        COMMENTS.append(ln.strip('\n'))
sep = '|S|P|E|C|I|A|L|S|E|P|'
with open(DATA_PATH + 'htmls.txt', encoding='utf-8', errors='replace') as f:
    data = f.read()
    HTMLS = data.split(sep)
with open(DATA_PATH + 'urls.txt', 'r', encoding='utf-8', errors='replace') as f:
    URL_LINES = f.read().splitlines()


_scrape_config = {
    'twitter': {
        'a|b|c|d': {
            'use_proxies': False,
            'topic1': ['q1', 'q2']
        }
    },
    'reddit': {
        'a|b': {
            'topic1': ['q1', 'q2']
        }
    },
    'blog': {
        'a': {
            'topic1': ['q1', 'q2']
        }
    },
    'article': {
        '': {
            'topic1': ['q1', 'q2']
        }
    }

}


def js(d):
    async def wr():
        return d
    return wr


def _resp(dct):
    if not dct.get('raise_for_status', None):
        dct.update(raise_for_status=lambda *a: None)
    return type('', (), dct)


class _AGen:
    def __init__(self, items=None):
        self.data = items or ['test%d' % k for k in range(10)]
        self.items = None

    def __aiter__(self):
        self.items = iter(self.data)
        return self

    async def __anext__(self):
        await asyncio.sleep(1e-5)
        try:
            return next(self.items)
        except StopIteration:
            raise StopAsyncIteration


_token = _resp({'json': js({'access_token': 'abc_test_abc'})})
_fake_requests = {
    'oauth2': _token,
    'fstream': _resp({'content': _AGen()}),
    'fail': _resp({'text': js('failed'), 'status': 404}),
    'fetch': _resp({'text': js('test')}),
    'https://www.reddit.com/api/v1/access_token': _token,
    'twitter': _resp({
        'content': _AGen([
            b'{"created_at":"Sat May 26 16:41:11 +0000 2018","text":"some data 2","created_at":"Fri Oct 28 22:38:06 +0000 2016","timestamp_ms":"1527352873612"}\r',
            b'{"created_at":"Sat May 26 16:41:13 +0000 2018","text":"some data 2","created_at":"Fri Jun 13 16:06:01 +0000 2014","timestamp_ms":"1527352873612"}\r'
        ])
    }),
    'reddit': _resp({
        'text': js('{"data": {"children": [{"data": {"id": "id"}}]}}')
    }),
    'reddit_comments': _resp({
        'text': js('[0, {"data": {"children": [{"kind": "t1", "data": {"body": "some data", "created_utc": 0.0}}]}}]')
    }),
    'twingly': _resp({
        'text': js("""<?xml version="1.0" encoding="utf-8"?><twinglydata numberOfMatchesReturned="1" secondsElapsed="0.24" numberOfMatchesTotal="1674" incompleteResult="false"><post><id>6088893520704939939</id><author>Ric</author><url>http://testurl.com/stuff.html</url><title>GMO Internet Group Launches First-ever 7nm Mining Rig</title><text>some data</text><languageCode>en</languageCode><locationCode>us</locationCode><coordinates /><links><link>https://www.cryptomartez.com/2018/05/gmo-coin-borrows-bch-eth-ltc-xrp.html</link><link>https://www.gmo.jp/en/news/article/?id=777</link><link>https://www.addtoany.com/add_to/facebook?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/pocket?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/twitter?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/google_plus?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/telegram?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/pinterest?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/reddit?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/copy_link?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.cryptomartez.com/2018/05/gmo-internet-group-7nm-mining-rig.html</link><link>https://www.cryptomartez.com/</link></links><tags /><images /><indexedAt>2018-05-26T19:39:26Z</indexedAt><publishedAt>2018-05-26T19:38:00Z</publishedAt><reindexedAt>2018-05-26T19:39:26Z</reindexedAt><inlinksCount>0</inlinksCount><blogId>10741039029997409540</blogId><blogName>Earn Bitcoin Work</blogName><blogUrl>https://www.earnbitcoin.work/</blogUrl><blogRank>1</blogRank><authority>0</authority></post></twinglydata>""")
    }),
    'google': _resp({
        'text': js("""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Test</title>
</head>
<body>
<a href="http://testurl.com/stuff.html"></a>
</body>
</html>
""")
    }),
    'http://testurl.com/stuff.html': _resp({
        'text': js("""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Test</title>
</head>
<body>
<p>Some data is a brewing, some data is a brewing!</p>
</body>
</html>
""")
    })
}


class RequestContextManager:
    def __init__(self, coro):
        self._resp = type('', (), {'release': lambda *a: None})

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


async def fake_request(self, method, url, **kwargs):
    data_collect = kwargs.pop('_data_collect', [])
    data_collect.append((method, url, kwargs))
    try:
        return _fake_requests[url]
    except KeyError:
        if url == 'user':
            return kwargs.pop('headers', {}).pop('user-agent', None)

        elif url in ['error', 'exception']:
            def fail():
                if url == 'error':
                    e = aiohttp.ClientResponseError({}, [])
                else:
                    e = TypeError
                raise e
            r = _fake_requests['fail']
            r.raise_for_status = fail
            return r

        elif url.startswith('https://stream.twitter.com'):
            return _fake_requests['twitter']

        elif url.startswith('https://oauth.reddit.com/'):
            if 'comments' in url:
                return _fake_requests['reddit_comments']
            else:
                return _fake_requests['reddit']

        elif url.startswith('https://api.twingly.com/'):
            return _fake_requests['twingly']

        elif url.startswith('https://news.google.com'):
            return _fake_requests['google']


@contextmanager
def _patched_aiohttp(monkeypatch):
    saved_exit = aiohttp.client._RequestContextManager.__aexit__
    saved_req = aiohttp.ClientSession._request
    monkeypatch.setattr(aiohttp.ClientSession, '_request', fake_request)
    monkeypatch.setattr(aiohttp.client._RequestContextManager,
                        '__aexit__', RequestContextManager.__aexit__)

    yield

    monkeypatch.setattr(aiohttp.ClientSession,
                        '_request', saved_req)
    monkeypatch.setattr(aiohttp.client._RequestContextManager,
                        '__aexit__', saved_exit)


class TestScraper(Scraper):
    def __init__(self, *args, **kwargs):
        super(TestScraper, self).__init__(*args, **kwargs)
        self.calls = []

    async def scrape(self, query, topic='', **kwargs):
        self.calls.append((query, topic, kwargs))
        for k in range(5):
            await self.queues[topic].put('test%d' % k)


class TestGen(ItemGenerator):
    def process_text(self, text):
        return text[0]

    def process_time(self, text):
        return text[1]


class TestHTMLScraper(HTMLScraper):
    calls = []
    item_gen = TestGen

    def extract_urls(self, text):
        urls = text.split('\n')
        return urls, list(range(len(urls)))

    def query_string(self, query):
        return query


class TestProxyPool:
    def __init__(self, q):
        self.q = q

    async def get(self, *args, **kwargs):
        return await self.q.get()


class TestProxyBroker:
    def __init__(self, queue=None, loop=None):
        self.q = queue

    async def find(self, *args, **kwargs):
        for _ in range(1000):
            await self.q.put(type('', (), {'host': '127.0.0.1', 'port': 5000}))
        await asyncio.sleep(1)


class TestRedis:
    data = defaultdict(deque)

    def __init__(self, *args, **kwargs):
        pass

    def rpush(self, key, d):
        self.data[key].append(d)

    def lpop(self, key):
        return self.data[key].popleft()


@pytest.fixture
def static_data():
    data_dict = {
        'tweets': TWEETS, 'comments': COMMENTS,
        'htmls': HTMLS, 'url_lines': URL_LINES
    }
    return lambda t: data_dict[t]


@pytest.fixture
def assert_rate_limited():
    async def wrapper(sess, n, url, t, places):
        last_request = time.time()
        for i in range(n):
            async with sess.request('GET', url):
                now = time.time()
                assert round(now - last_request, places) == t, \
                    'Blocking rate waited while not limited'
                last_request = now
    return wrapper


@pytest.fixture
def patched_aiohttp(monkeypatch):
    with _patched_aiohttp(monkeypatch):
        yield


@pytest.fixture
def patched_session(monkeypatch):
    with _patched_aiohttp(monkeypatch):
        yield veryscrape.session.Session


@pytest.fixture
def patched_oauth1(monkeypatch):
    with _patched_aiohttp(monkeypatch):
        yield veryscrape.session.OAuth1Session


@pytest.fixture
def patched_oauth2(monkeypatch):
    with _patched_aiohttp(monkeypatch):
        yield veryscrape.session.OAuth2Session


@pytest.fixture
def scraper():
    return partial(TestScraper, veryscrape.session.Session)


@pytest.fixture
def html_scraper(request):
    # this injection is a bit messy
    old_fetch = veryscrape.scrape.fetch

    async def _add(method, url, **kwargs):
        TestHTMLScraper.calls.append((method, url, kwargs))
        if url == 'urls':
            return 'http://example.com\nhttps://example.com'
        else:
            return '<html>stuff%d</html>' % len(TestHTMLScraper.calls)
    veryscrape.scrape.fetch = _add
    request.addfinalizer(TestHTMLScraper.calls.clear)

    yield partial(TestHTMLScraper, veryscrape.session.Session)

    veryscrape.scrape.fetch = old_fetch


@pytest.fixture
def patched_proxy_pool(monkeypatch):
    monkeypatch.setattr('veryscrape.veryscrape.ProxyPool', TestProxyPool)
    monkeypatch.setattr('veryscrape.veryscrape.Broker', TestProxyBroker)
    yield


@pytest.fixture
def scrape_config():
    return _scrape_config.copy()


@pytest.fixture
def temp_config():
    with open('scrape_config.json', 'w') as fle:
        json.dump(_scrape_config, fle)
    yield
    os.remove('scrape_config.json')


@pytest.fixture
def patched_redis(monkeypatch):
    monkeypatch.setattr('veryscrape.cli.Redis', TestRedis)
    yield TestRedis()

