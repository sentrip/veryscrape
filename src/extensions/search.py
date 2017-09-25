# Class to stream text data from Google's services (news) and Twingly's services (blog)
import asyncio
import os
import re
import time
import ssl
from collections import deque
from queue import Empty
from urllib.parse import urlencode

import aiohttp
from fake_useragent import UserAgent
from lxml import html
from twingly_search.parser import Parser

from src.base import BASE_DIR, Item, AsyncStream, ExponentialBackOff
from src.extensions.proxy import random_proxy


class DownloadStream:
    def __init__(self, url_queue, result_queue):
        self.session = None
        self.url_queue = url_queue
        self.result_queue = result_queue
        self.fake_users = UserAgent()

    async def download(self, item):
        retry_counter = ExponentialBackOff()
        async for _ in retry_counter:
            try:
                async with self.session.get(item.content, headers={'user-agent': self.fake_users.random}) as response:
                    enc_search = re.search('charset=(?P<enc>\S*)', response.headers.get('content-type', default=''))
                    encoding = enc_search.group('enc') if enc_search else 'UTF-8'
                    html_text = await response.text(encoding=encoding)
                if response.status == 200:
                    self.result_queue.put(Item(html_text, item.topic, item.source))
                    break
            except aiohttp.client_exceptions.ClientError:
                self.session.close()
                self.session = aiohttp.ClientSession()
            except (aiohttp.client_exceptions.TimeoutError, ssl.CertificateError, KeyError):
                pass
            except UnicodeDecodeError:
                break
            except Exception as e:
                print('Download', repr(e))

    async def stream(self):
        self.session = aiohttp.ClientSession()
        while True:
            try:
                url = self.url_queue.get_nowait()
                asyncio.ensure_future(self.download(url))
            except Empty:
                await asyncio.sleep(0.1)


class SearchClient:
    source = 'source'
    name = 'client'
    proxy_params = {'minDownloadSpeed': '50', 'protocol': 'http', 'allowsHttps': 1}
    bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
    false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}

    def __init__(self, user_agent=None):
        self.user_agent = UserAgent().random if user_agent is None else user_agent
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent})
        self.retry_counter = ExponentialBackOff()

    def build_query(self, q):
        return q

    @staticmethod
    def urls_generator(result):
        yield result

    def clean_urls(self, urls):
        for i in self.urls_generator(urls):
            is_root_url = any(i.endswith(j) for j in self.bad_domains)
            is_not_relevant = any(j in i for j in self.false_urls)
            if i.startswith('http') and not (is_root_url or is_not_relevant):
                yield i

    async def fetch(self, url):
        resp = None
        async for _ in self.retry_counter:
            try:
                if self.source == 'article':
                    proxy = await random_proxy(**self.proxy_params)
                    async with self.session.get(url, proxy=proxy) as response:
                        resp = await response.text()
                else:
                    async with self.session.get(url) as response:
                        resp = await response.text()
                self.retry_counter.reset()
                break
            except aiohttp.client_exceptions.ClientError:
                self.session.close()
                self.session = aiohttp.ClientSession(headers={'user-agent': self.user_agent})
            except (aiohttp.client_exceptions.TimeoutError, KeyError):
                pass
            except Exception as e:
                print(self.name, repr(e))
        return resp

    async def search(self, q):
        """Executes the given search query and returns the result"""
        query_url = self.build_query(q)
        scraped_urls = set()
        r = await self.fetch(query_url)
        for url in self.clean_urls(r):
            scraped_urls.add(url)
        return scraped_urls


class TwinglyClient(SearchClient):
    """Custom Twingly Search API client"""
    SEARCH_API_VERSION = "v3"
    API_URL = "https://api.twingly.com/blog/search/api/%s/search" % SEARCH_API_VERSION
    name = 'Twingly'
    source = 'blog'

    def __init__(self, api_key=None):
        super(TwinglyClient, self).__init__("Twingly Search Python Client/2.1.0")
        self.api_key = api_key
        self.clock = 0

    @staticmethod
    def load_authentications():
        """Returns oldest functional/unused twingly api key from disk"""
        with open(os.path.join(BASE_DIR, 'lib', 'api', 'twingly.txt')) as f:
            for ln in f:
                status, key = ln.strip('\n').split(',')
                if status == 'NOT_USED' or time.time() - float(status) < 13.75 * 24 * 3600:
                    return key
            else:
                raise KeyError('There are no usable Twingly api-keys left, please repopulate')

    def build_query(self, q):
        if self.api_key is None or time.time() - self.clock >= 3600:
            self.api_key = self.load_authentications()
            self.clock = time.time()
        query_string = q + " lang:en" + ' tspan:12h page-size:10000'
        return "%s?%s" % (self.API_URL, urlencode({'q': query_string, 'apikey': self.api_key}))

    @staticmethod
    def urls_generator(h):
        result = Parser().parse(h)
        for post in result.posts:
            yield post.url


class GoogleClient(SearchClient):
    """Custom Google News Search client"""
    API_URL = 'https://news.google.com/news/search/section/q/{}/{}?hl=en&ned=us'
    name = 'Google'
    source = 'article'

    def build_query(self, q):
        return self.API_URL.format(q, q)

    @staticmethod
    def urls_generator(h):
        result = html.fromstring(h)
        for e in result.xpath('//*[@href]'):
            yield e.get('href') if e.get('href') is not None else ''


class SearchStream(AsyncStream):
    def __init__(self, client_type, topic, queries, queue, search_every=900):
        self.client = None
        self.client_type = client_type
        self.topic = topic
        self.queries = queries
        self.queue = queue
        self.search_every = search_every
        self.infinite_queries = iter(self.queries)
        self.seen_urls = deque(maxlen=100000)

    async def __anext__(self):
        if not self.client:
            if self.client_type == 'article':
                self.client = GoogleClient()
            elif self.client_type == 'blog':
                self.client = TwinglyClient()
        try:
            q = next(self.infinite_queries)
        except StopIteration:
            await asyncio.sleep(self.search_every)
            self.infinite_queries = iter(self.queries)
            q = next(self.infinite_queries)

        urls = await self.client.search(q)
        for url in urls:
            if url not in self.seen_urls:
                self.queue.put(Item(url, self.topic, self.client.source))
                self.seen_urls.append(url)
        return
