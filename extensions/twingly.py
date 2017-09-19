# Class for streaming blog texts from twingly search
import asyncio
import os
import time
from collections import deque
from urllib.parse import urlencode

import aiohttp
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
        query_string = q + " lang:en" + ' tspan:12h page-size:10000'
        query_url = "%s?%s" % (self.API_URL, urlencode({'q': query_string, 'apikey': self.api_key}))
        async with self.session.get(query_url) as response:  # proxy=self.proxy
            raw = await response.text()
        result = self.parser.parse(raw)
        return result

async def twingly(parent, topic, query, search_every=15*60):
    bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
    false_urls = {'google.', 'youtube.'}
    retry_time = 10.0
    api_key = load_authentications(os.path.join(BASE_DIR, 'lib', 'api', 'twingly.txt'))
    client = TwinglyClient(api_key, None)
    seen_urls = deque(maxlen=100000)
    start_time = time.time()

    while parent.running:
        try:
            result = await client.execute_query(query)
            for post in result.posts:
                # Remove any useless urls
                is_root_url = any(post.url.endswith(i) for i in bad_domains)
                is_not_relevant = any(i in post.url for i in false_urls)
                if not (is_root_url or is_not_relevant) and post.url not in seen_urls:
                    new_item = Item(content=post.url, topic=topic, source='blog')
                    seen_urls.append(post.url)
                    parent.url_queue.put(new_item)
            await asyncio.sleep(search_every)

        except Exception as e:
            print('Twingly', repr(e))
            await asyncio.sleep(retry_time)
            client = TwinglyClient(api_key, None)

        if time.time() - start_time >= 3600:
            api_key = load_authentications(os.path.join(BASE_DIR, 'lib', 'api', 'twingly.txt'))
            start_time = time.time()
    client.session.close()
