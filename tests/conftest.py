import os, sys
sys.path.append(os.path.abspath('../veryscrape'))
from contextlib import contextmanager
from collections import defaultdict, deque
from datetime import datetime
from functools import partial
import asyncio
import aiohttp
import json
import pytest
import random
import re
import time
import veryscrape.session
from veryscrape.items import Item, ItemGenerator
from veryscrape.scrape import Scraper, SearchEngineScraper


# ===================================== #
#          Internet Mocking             #
# ===================================== #


class FakeStreamReader:
    def __init__(self, data):
        self.data = data

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0)
        idx = self.data.find(b'\n')
        if idx < 0:
            raise StopAsyncIteration
        msg, self.data = self.data[:idx], self.data[idx+1:]
        return msg

    async def read(self):
        data = b''
        async for item in self:
            data += item
        return data


class FakeResponse:
    def __init__(self, method, url, status, body):
        self.method = method
        self._url = url
        self.reason = 'OK'
        self.version = '1.1'
        self.status = status
        self._body = body

    async def json(self, **kwargs):
        return json.loads(self._body.decode(encoding='utf-8'))

    async def text(self, **kwargs):
        return self._body.decode(encoding='utf-8')

    @property
    def content(self):
        return FakeStreamReader(self._body)

    def raise_for_status(self):
        if self.status != 200:
            raise aiohttp.ClientResponseError({}, [], status=200)


class FakeInternet:

    _responses = defaultdict(dict)

    @staticmethod
    def html(body, title='Title'):
        if isinstance(body, list):
            body = '\n'.join(['<a href="%s"></a>' % lnk for lnk in body])

        _html = """<!doctype html>\n\t<html lang="en">\n\t<head>
        <meta charset="utf-8">\n\t\t<title>%s</title>\n\t</head>\n\t<body>
    %s
    </body>\n</html>""" % (title, body)
        return _html.encode('utf-8')

    @staticmethod
    def register(method='GET', path='.*', status=200, data=None, sleep_time=0.):
        if data is None:
            data = b''
        elif isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')
        FakeInternet._responses[method][path] = (status, data, sleep_time)

    @staticmethod
    async def get_response_data(method, url):
        dct = FakeInternet._responses[method].items()
        for path, (status, data, sleep_time) in dct:
            if re.search(path, url):
                break
        else:
            status, data, sleep_time = 200, b'', 0

        await asyncio.sleep(sleep_time)
        return status, data

    async def request(self, method, url, **kwargs):
        data_collect = kwargs.pop('_data_collect', [])
        data_collect.append((method, url, kwargs))
        status, data = await FakeInternet.get_response_data(method, url)
        if callable(data):
            data = data(**kwargs)
        return FakeResponse(method, url, status, data)


# ===================================== #
#          Test Helper Classes          #
# ===================================== #


class RequestContextManager:
    def __init__(self, coro):
        self._resp = type('', (), {'release': lambda *a: None})

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


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


class TestHTMLScraper(SearchEngineScraper):
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


class TestItemGen(ItemGenerator):
    def process_text(self, text):
        return text[0]

    def process_time(self, text):
        return text[1]


def _random_items():
    n_items = 100
    now = time.time()
    items = []
    for k in range(n_items):
        items.append(Item(k, created_at=datetime.fromtimestamp(now - n_items + k)))
    random.shuffle(items)
    return items


@contextmanager
def _patched_aiohttp(monkeypatch):
    saved_exit = aiohttp.client._RequestContextManager.__aexit__
    saved_req = aiohttp.ClientSession._request
    monkeypatch.setattr(aiohttp.ClientSession, '_request', FakeInternet.request)
    monkeypatch.setattr(aiohttp.client._RequestContextManager,
                        '__aexit__', RequestContextManager.__aexit__)

    yield

    monkeypatch.setattr(aiohttp.ClientSession,
                        '_request', saved_req)
    monkeypatch.setattr(aiohttp.client._RequestContextManager,
                        '__aexit__', saved_exit)


# ===================================== #
#               Fixtures                #
# ===================================== #


@pytest.fixture
def assert_rate_limited():
    async def wrapper(sess, n, url, t, places):
        # on slow systems sometimes the rate limit is not exact
        # so this is one less to make sure tests always pass
        places -= 1
        last_request = time.time()
        for _ in range(n):
            async with sess.request('GET', url):
                now = time.time()
                assert round(now - last_request, places) == round(t, places), \
                    'Blocking rate limited request waited while not limited'
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
def patched_redis(monkeypatch):
    monkeypatch.setattr('veryscrape.cli.Redis', TestRedis)
    yield TestRedis()


@pytest.fixture
def random_item_gen():
    q = asyncio.Queue()
    for item in _random_items():
        q.put_nowait((item.content, item.created_at))
    return TestItemGen(q)


@pytest.fixture
def random_item_queue():
    q = asyncio.Queue()
    for item in _random_items():
        q.put_nowait(item)
    return q


@pytest.fixture
def scrape_config():
    return _scrape_config.copy()


@pytest.fixture
def static_data():
    data_dict = {
        'tweets': TWEETS, 'comments': COMMENTS,
        'htmls': HTMLS, 'url_lines': URL_LINES
    }
    return lambda t: data_dict[t]


@pytest.fixture
def temp_config():
    with open('scrape_config.json', 'w') as fle:
        json.dump(_scrape_config, fle)
    yield
    os.remove('scrape_config.json')


# ===================================== #
# Data loading and fake internet config #
# ===================================== #

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
    _data = f.read()
    HTMLS = _data.split(sep)
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


def spider_html(**kwargs):
    # this needs to stay, DON'T change these values
    # mocking an arbitrarily-deep website
    # is complicated without this trick
    if random.random() < 0.76:
        return data_html
    else:
        body = b'\n'.join([b'<a href="spider%d"></a>' % random.randint(0, 1e6) for _ in range(4)])
        return FakeInternet.html(_brewing_string.encode() + b'\n' + body)


_brewing_string = '<p>Some data is a brewing, some data is a brewing!</p>'
data_html = FakeInternet.html(_brewing_string)
token = {'access_token': 'abc_test_abc'}
FakeInternet.register(path='spider', data=spider_html)
FakeInternet.register(method='POST', path='oauth2', data=token)
FakeInternet.register(method='POST', path=r'reddit.com/api/v1/access_token', data=token)
FakeInternet.register(path='fstream', data=b'\n'.join([b'test%d' % i for i in range(10)]))
FakeInternet.register(path='fail', data=b'failed', status=404)
FakeInternet.register(path='fetch', data=b'test')
FakeInternet.register(path='user', data=lambda **k: k.pop('headers', {}).pop('user-agent', None))
FakeInternet.register(path='error', data=b'failed', status=400)
FakeInternet.register(path='exception', data=1)  # data not bytes causes error
FakeInternet.register(path=r'testurl\.com/stuff\.html', data=data_html)
FakeInternet.register(path=r'news\.google\.com', data=FakeInternet.html(['http://testurl.com/stuff.html']))
FakeInternet.register(path=r'oauth\.reddit\.com/r/\w+/\w+\.json', data={"data": {"children": [{"data": {"id": "id"}}]}})
FakeInternet.register(path=r'oauth\.reddit\.com/r/\w+/comments/\w+\.json',
                      data=b'[0, {"data": {"children": [{"kind": "t1", '
                           b'"data": {"body": "some data", "created_utc": 0.0}}]}}]')
FakeInternet.register(method='POST', path=r'stream\.twitter\.com',
                      data=b'{"created_at":"Sat May 26 16:41:11 +0000 2018","text":"some data 2",'
                           b'"created_at":"Fri Oct 28 22:38:06 +0000 2016","timestamp_ms":"1527352873612"}\r\n'
                           b'{"created_at":"Sat May 26 16:41:13 +0000 2018","text":"some data 2",'
                           b'"created_at":"Fri Jun 13 16:06:01 +0000 2014","timestamp_ms":"1527352873612"}\r')
FakeInternet.register(path=r'api\.twingly\.com',
                      data="""<?xml version="1.0" encoding="utf-8"?><twinglydata numberOfMatchesReturned="1" secondsElap
sed="0.24" numberOfMatchesTotal="1674" incompleteResult="false"><post><id>6088893520704939939</id><author>Ric</author><u
rl>http://testurl.com/stuff.html</url><title>GMO Internet Group Launches First-ever 7nm Mining Rig</title><text>some dat
a</text><languageCode>en</languageCode><locationCode>us</locationCode><coordinates /><links><link>https://www.cryptomart
ez.com/2018/05/gmo-coin-borrows-bch-eth-ltc-xrp.html</link><link>https://www.gmo.jp/en/news/article/?id=777</link><link>
https://www.addtoany.com/add_to/facebook?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-m
ining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/a
dd_to/pocket?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=
GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/twitter?linkurl=https:
%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches
First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/google_plus?linkurl=https:%2F%2Fwww.cryptomartez.c
om%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining R
ig</link><link>https://www.addtoany.com/add_to/telegram?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-inter
net-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www
.addtoany.com/add_to/pinterest?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.
html&amp;linkname=GMO Internet Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/redd
it?linkurl=https:%2F%2Fwww.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Intern
et Group Launches First-ever 7nm Mining Rig</link><link>https://www.addtoany.com/add_to/copy_link?linkurl=https:%2F%2Fww
w.cryptomartez.com%2F2018%2F05%2Fgmo-internet-group-7nm-mining-rig.html&amp;linkname=GMO Internet Group Launches First-e
ver 7nm Mining Rig</link><link>https://www.cryptomartez.com/2018/05/gmo-internet-group-7nm-mining-rig.html</link><link>h
ttps://www.cryptomartez.com/</link></links><tags /><images /><indexedAt>2018-05-26T19:39:26Z</indexedAt><publishedAt>201
8-05-26T19:38:00Z</publishedAt><reindexedAt>2018-05-26T19:39:26Z</reindexedAt><inlinksCount>0</inlinksCount><blogId>107
41039029997409540</blogId><blogName>Earn Bitcoin Work</blogName><blogUrl>https://www.earnbitcoin.work/</blogUrl><blogRa
nk>1</blogRank><authority>0</authority></post></twinglydata>""".replace('\n', '').encode('utf-8'))
