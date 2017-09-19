# Class for streaming blog texts from twingly search
import aiohttp
import asyncio
import datetime
import os
import time
from collections import deque
from pytz import utc
from queue import Queue
from threading import Thread
from urllib.parse import urlencode

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

    async def execute_query(self, q, hours_since=3):
        """Executes the given search query and returns the result"""
        end_time = datetime.datetime.utcnow()
        start_time = end_time - datetime.timedelta(hours=hours_since)
        time_strings = tuple(map(lambda x: x.astimezone(utc).strftime('%Y-%m-%dT%H:%M:%SZ'), (start_time, end_time)))
        query_string = q + " lang:en" + " start-date:{} end-date:{}".format(*time_strings)
        url_parameters = urlencode({'q': query_string, 'apiKey': self.api_key})
        query_url = "%s?%s" % (self.API_URL, url_parameters)
        response = await self.session.get(query_url, proxy=self.proxy).content
        result = self.parser.parse(response)
        return result

async def twingly(parent, topic, query, search_every=15*60):
    bad_domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
    false_urls = {'www.google', '.google.', '.youtube.'}
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


class TwinglyStream(Source):
    def __init__(self, parent, outgoing, proxy_thread):
        super(TwinglyStream, self)._init__(parent, outgoing, proxy_thread=proxy_thread, output_via_queue=True)
        self.api_path = os.path.join(self.base_dir, 'lib', 'api', 'twingly.txt')
        self.api = load_authentications(self.api_path)
        self.url_queue = Queue()
        DownloadStream(self.url_queue, self.queue).start()
        Thread(target=self.update_api_keys).start()

    def check_update_apis(self):
        """Checks api keys in file, updates unused keys and removes keys older than 2 weeks"""
        time.sleep(3600)
        apis = []
        with open(self.api_path) as f:
            for ln in f:
                status, key = ln.strip('\n').split(',')
                if key == self.api:
                    if status == 'NOT_USED':
                        apis.append((time.time(), key))
                    elif time.time() - float(status) > 13.75 * 24 * 3600:
                        self.api = load_authentications(self.api_path)
                elif status == 'NOT_USED' or time.time() - float(status) < 13.75 * 24 * 3600:
                    apis.append((status, key))
        with open(self.api_path, 'w') as f:
            for pair in apis:
                f.write('{},{}\n'.format(*pair))

    def update_api_keys(self):
        """Checks every hour whether any api keys are outdated and removes them"""
        while self.parent.running:
            start = time.time()
            self.check_update_apis()
            time.sleep(max(0, 3600 - (time.time()-start)))

    def create_stream(self, topic, proxy):
        new_stream = TwinglyTopicStream(self, self.api, topic, proxy)
        return new_stream
