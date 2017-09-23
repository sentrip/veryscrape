# Class to stream text data from Google's services (news) and Twingly's services (blog)
import os
import random
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from multiprocessing import Process
from threading import Thread
from urllib.parse import urlencode

import requests
from fake_useragent import UserAgent
from lxml import html
from twingly_search import parser

from base import BASE_DIR, Item


class SearchClient:
    source = 'source'
    name = 'client'

    def __init__(self, user_agent):
        self.session = requests.Session()
        self.session.headers = {'User-Agent': user_agent}

    def build_query(self, q):
        return q

    @staticmethod
    def urls_generator(result):
        yield result

    def clean_urls(self, urls):
        domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        for i in self.urls_generator(urls):
            is_root_url = any(i.endswith(j) for j in domains)
            is_not_relevant = any(j in i for j in false_urls)
            if i.startswith('http') and not (is_root_url or is_not_relevant):
                yield i

    def fetch_url(self, url, proxy):
        self.session.proxies = proxy
        return self.session.get(url).content

    def search(self, q, proxy):
        """Executes the given search query and returns the result"""
        query_url = self.build_query(q)
        raw = None
        count = 0
        scraped_urls = set()
        while not raw and count < 5:
            try:
                raw = self.fetch_url(query_url, proxy)
                for url in self.clean_urls(raw):
                    scraped_urls.add(url)
            except (requests.exceptions.ProxyError, requests.exceptions.SSLError):
                time.sleep(random.random()*2)
                count += 1
        return scraped_urls


class TwinglyClient(SearchClient):
    """Custom Twingly Search API client"""
    SEARCH_API_VERSION = "v3"
    API_URL = "https://api.twingly.com/blog/search/api/%s/search" % SEARCH_API_VERSION
    name = 'Twingly'
    source = 'blog'

    def __init__(self, api_key):
        super(TwinglyClient, self).__init__("Twingly Search Python Client/2.1.0")
        self.api_key = api_key or self.load_authentications()

    @staticmethod
    def load_authentications():
        """Returns oldest functional/unused twingly api key from disk"""
        with open(os.path.join(BASE_DIR, 'lib', 'api', 'twingly.txt')) as f:
            for ln in f:
                status, key = ln.strip('\n').split(',')
                if status == 'NOT_USED':
                    return key
                elif time.time() - float(status) < 13.75 * 24 * 3600:
                    return key
            else:
                raise KeyError('There are no usable Twingly api-keys left, please repopulate')

    def build_query(self, q):
        query_string = q + " lang:en" + ' tspan:12h page-size:10000'
        return "%s?%s" % (self.API_URL, urlencode({'q': query_string, 'apikey': self.api_key}))

    @staticmethod
    def urls_generator(h):
        result = parser.Parser().parse(h)
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


class NewsStream(Process):
    def __init__(self, parent):
        super(NewsStream, self).__init__()
        self.parent = parent
        self.proxies = parent.proxy_thread
        self.fake_users = UserAgent()
        self.seen = deque(maxlen=100000)
        self.google = GoogleClient(self.fake_users.random)
        self.twingly = TwinglyClient(None)
        self.sess = requests.Session()
        self.expiry_time = time.time()
        self.pool = ThreadPoolExecutor(len(parent.topics))
        self.download_pool = ThreadPoolExecutor(len(parent.topics) * 5)

    def single_download(self, item):
        with suppress(requests.exceptions.SSLError):
            h = self.sess.get(item.content, headers={'user-agent': UserAgent().random}).content
            self.parent.result_queue.put(Item(str(h), item.topic, item.source))

    def download(self):
        """Async downloading of html text from article urls"""
        while self.parent.running:
            if not self.parent.url_queue.empty():
                item = self.parent.url_queue.get_nowait()
                self.download_pool.submit(self.single_download, item)

    def client_search(self, client, topic, query, proxy=None):
        success = False
        ex = False
        while not success:
            try:
                p = proxy if not ex else (self.proxies.random(client.source, True) if proxy is not None else proxy)
                urls = client.search(query, p)
                for url in urls:
                    if url not in self.seen:
                        self.seen.append(url)
                        self.parent.url_queue.put(Item(url, topic, client.source))
                success = True
            except requests.exceptions.ChunkedEncodingError:
                ex = True
            except Exception as e:
                print(client.name, repr(e))
                ex = True

    def run(self):
        clock = time.time()
        Thread(target=self.download).start()
        while self.parent.running:
            start_time = time.time()
            for topic in self.parent.topics:
                proxy = self.proxies.random('article', True)
                for query in self.parent.topics[topic]:
                    self.pool.submit(self.client_search, self.google, topic, query, proxy)
                    self.pool.submit(self.client_search, self.twingly, topic, query)
            time.sleep(max(0, 15 * 60 - (time.time() - start_time)))
            if time.time() - clock >= 3600:
                api_key = self.twingly.load_authentications()
                self.twingly = TwinglyClient(api_key)
                clock = time.time()
        self.twingly.session.close()
        self.google.session.close()
        self.sess.close()
        self.pool.shutdown()
