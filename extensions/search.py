# Class to stream text data from Google's services (news) and Twingly's services (blog)
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from urllib.parse import urlencode

import requests
from fake_useragent import UserAgent
from lxml import html
from twingly_search import parser

from base import BASE_DIR, Item, SearchClient


class TwinglyClient(SearchClient):
    """Custom Twingly Search API client"""
    SEARCH_API_VERSION = "v3"
    API_URL = "https://api.twingly.com/blog/search/api/%s/search" % SEARCH_API_VERSION
    name = 'Twingly'

    def __init__(self, api_key, async):
        super(TwinglyClient, self).__init__("Twingly Search Python Client/2.1.0", async)
        self.api_key = api_key or self.load_authentications()
        self.source = 'blog'

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


def client_search(client, topic, query, seen_urls, queue, proxy, proxy_thread):
    success = False
    ex = False
    while not success:
        try:
            urls = client.execute_query_no_async(query, proxy if not ex else proxy_thread.random(client.source, True))
            for url in urls:
                if url not in seen_urls:
                    new_item = Item(content=url, topic=topic, source=client.source)
                    seen_urls.append(url)
                    queue.put(new_item)
            success = True
        except (requests.exceptions.ProxyError, requests.exceptions.SSLError):
            ex = True
        except Exception as e:
            print(client.name, repr(e))
            ex = True


def single_download(parent, sess, item):
    try:
        response = sess.get(item.content, headers={'user-agent': UserAgent().random})
        parent.result_queue.put(Item(str(response.content), item.topic, item.source))
    except:
        pass


def download(pool, parent):
    """Async downloading of html text from article urls"""
    sess = requests.Session()
    while parent.running:
        if not parent.url_queue.empty():
            item = parent.url_queue.get_nowait()
            pool.submit(single_download, parent, sess, item)
    sess.close()


def search(parent, proxy_thread, search_every=15*60):
    fake_users = UserAgent()
    seen_urls = deque(maxlen=100000)
    google = GoogleClient(fake_users.random, False)
    twingly = TwinglyClient(None, False)
    expiry_time = time.time()
    pool = ThreadPoolExecutor(100 + len(parent.topics))
    Thread(target=download, args=(pool, parent, )).start()
    while parent.running:
        start_time = time.time()
        for topic in parent.topics:
            google_proxy = proxy_thread.random('article', True)
            twingly_proxy = proxy_thread.random('blog', True)
            for query in parent.topics[topic]:
                pool.submit(client_search, twingly, topic, query,
                            seen_urls, parent.url_queue, twingly_proxy, proxy_thread)
                pool.submit(client_search, google, topic, query,
                            seen_urls, parent.url_queue, google_proxy, proxy_thread)
        time.sleep(max(0, search_every - (time.time() - start_time)))
        if time.time() - expiry_time >= 3600:
            api_key = twingly.load_authentications()
            twingly = TwinglyClient(api_key, False)
            expiry_time = time.time()
    twingly.session.close()
    google.session.close()
