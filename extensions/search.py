# Class to stream text data from Google's services (news) and Twingly's services (blog)
import asyncio
import os
import time
from collections import deque
from urllib.parse import urlencode

import aiohttp
from fake_useragent import UserAgent
from lxml import html
from twingly_search import parser

from base import BASE_DIR, Item


def load_authentications(file_path):
    """Returns oldest functional/unused twingly api key from disk"""
    with open(file_path) as f:
        for ln in f:
            status, key = ln.strip('\n').split(',')
            if status == 'NOT_USED':
                return key
            elif time.time() - float(status) < 13.75 * 24 * 3600:
                return key
        else:
            raise KeyError('There are no usable Twingly api-keys left, please repopulate')


class TwinglyClient:
    """Custom Twingly Search API client"""
    SEARCH_API_VERSION = "v3"
    API_URL = "https://api.twingly.com/blog/search/api/%s/search" % SEARCH_API_VERSION

    def __init__(self, api_key, proxy):
        self.api_key = api_key
        self.parser = parser.Parser()
        self.user_agent = "Twingly Search Python Client/2.1.0"
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent})
        self.proxy = proxy

    async def execute_query(self, q):
        """Executes the given search query and returns the result"""
        bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'youtube.'}
        query_string = q + " lang:en" + ' tspan:12h page-size:10000'
        query_url = "%s?%s" % (self.API_URL, urlencode({'q': query_string, 'apikey': self.api_key}))
        async with self.session.get(query_url) as response:  # proxy=self.proxy
            raw = await response.text()
        result = self.parser.parse(raw)
        scraped_urls = set()
        for post in result.posts:
            # Remove any useless urls
            is_root_url = any(post.url.endswith(i) for i in bad_domains)
            is_not_relevant = any(i in post.url for i in false_urls)
            if not (is_root_url or is_not_relevant):
                scraped_urls.add(post.url)
        return scraped_urls


class GoogleClient:
    """Custom Google News Search client"""
    API_URL = 'https://news.google.com/news/search/section/q/{}/{}?hl=en&ned=us'

    def __init__(self, user_agent, proxy):
        self.user_agent = user_agent
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent})
        self.proxy = proxy

    async def execute_query(self, q):
        """Executes the given search query and returns the urls"""
        domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        query_url = self.API_URL.format(q, q)
        async with self.session.get(query_url) as response:  # proxy=self.proxy
            raw = await response.text()
        scraped_urls = set()
        try:
            html_tree = html.fromstring(raw)
            for e in html_tree.xpath('//*[@href]'):
                i = e.get('href') if e.get('href') is not None else ''
                is_root_url = any(i.endswith(j) for j in domains)
                is_not_relevant = any(j in i for j in false_urls)
                if i.startswith('http') and not (is_root_url or is_not_relevant):
                    scraped_urls.add(i)
        except Exception as e:
            pass
        return scraped_urls


async def search(parent, topic, query, search_every=15*60):
    fake_users = UserAgent()
    retry_time = 10.0
    seen_urls = deque(maxlen=100000)
    google = GoogleClient(fake_users.random, None)
    api_key = load_authentications(os.path.join(BASE_DIR, 'lib', 'api', 'twingly.txt'))
    twingly = TwinglyClient(api_key, None)
    expiry_time = time.time()

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
            api_key = load_authentications(os.path.join(BASE_DIR, 'lib', 'api', 'twingly.txt'))
            expiry_time = time.time()

    twingly.session.close()
    google.session.close()
