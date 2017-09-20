# Class to stream text data from Google's services (news) and Twingly's services (blog)
import asyncio
import os
import time
from collections import deque
from urllib.parse import urlencode

from fake_useragent import UserAgent
from lxml import html
from twingly_search import parser

from base import BASE_DIR, Item, SearchClient


class TwinglyClient(SearchClient):
    """Custom Twingly Search API client"""
    SEARCH_API_VERSION = "v3"
    API_URL = "https://api.twingly.com/blog/search/api/%s/search" % SEARCH_API_VERSION

    def __init__(self, api_key, proxy):
        super(TwinglyClient, self).__init__("Twingly Search Python Client/2.1.0", proxy)
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

    def build_query(self, q):
        return self.API_URL.format(q, q)

    @staticmethod
    def urls_generator(h):
        result = html.fromstring(h)
        for e in result.xpath('//*[@href]'):
            yield e.get('href') if e.get('href') is not None else ''

async def search(parent, topic, query, search_every=15*60):
    fake_users = UserAgent()
    retry_time = 10.0
    seen_urls = deque(maxlen=100000)
    google = GoogleClient(fake_users.random, None)
    twingly = TwinglyClient(None, None)
    expiry_time = time.time()
    api_key = twingly.api_key

    while parent.running:
        try:
            google_urls = await google.execute_query(query)
        except Exception as e:
            print('Google', repr(e))
            google_urls = []
            await asyncio.sleep(retry_time)
            google.session.close()
            google = GoogleClient(fake_users.random, None)
        try:
            twingly_urls = await twingly.execute_query(query)
        except Exception as e:
            print('Twingly', repr(e))
            twingly_urls = []
            await asyncio.sleep(retry_time)
            twingly.session.close()
            twingly = TwinglyClient(api_key, None)

        for url, url_type in zip([*google_urls, *twingly_urls], ['article']*len(google_urls) + ['blog']*len(twingly_urls)):
            if url not in seen_urls:
                new_item = Item(content=url, topic=topic, source=url_type)
                seen_urls.append(url)
                parent.url_queue.put(new_item)

        await asyncio.sleep(search_every)
        if time.time() - expiry_time >= 3600:
            api_key = twingly.load_authentications()
            expiry_time = time.time()

    twingly.session.close()
    google.session.close()
