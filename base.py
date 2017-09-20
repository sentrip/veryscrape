import asyncio
import os
import time
from hashlib import sha1
from multiprocessing import Process, Queue
from multiprocessing.connection import Listener
from random import SystemRandom
from threading import Thread

import aioauth_client
import aiohttp

random = SystemRandom().random
BASE_DIR = "/home/djordje/Sentrip/"
if not os.path.isdir(BASE_DIR):
    BASE_DIR = "C:/users/djordje/desktop"


class Item:
    """General container for text/numerical/vector data categorized by topic and source"""
    def __init__(self, content='', topic='', source=''):
        self.content = content
        self.topic = topic
        self.source = source

    def __repr__(self):
        return "Item({:5s}, {:7s}, {})".format(self.topic, self.source, str(self.content)[:10])

    def __hash__(self):
        return hash(str(self.content) + self.topic + self.source)

    def __eq__(self, other):
        return self.topic == other.topic and self.source == other.source and self.content == other.content


class AsyncOAuth(aioauth_client.Client):
    access_token_key = 'oauth_token'
    request_token_url = None
    version = '1.0'

    def __init__(self, consumer_key, consumer_secret, oauth_token=None, oauth_token_secret=None,
                 base_url=None, signature=None, **params):
        super().__init__(base_url, None, None, None, None)

        self.oauth_token = oauth_token
        self.oauth_token_secret = oauth_token_secret
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.params = params
        self.signature = signature or aioauth_client.HmacSha1Signature()
        self.sess = None

    async def request(self, method, url, params=None, headers=None, timeout=10, loop=None, **aio_kwargs):
        if not self.sess:
            self.sess = aiohttp.ClientSession()
        oparams = {
            'oauth_consumer_key': self.consumer_key,
            'oauth_nonce': sha1(str(random()).encode('ascii')).hexdigest(),
            'oauth_signature_method': self.signature.name,
            'oauth_timestamp': str(int(time.time())),
            'oauth_version': self.version}
        oparams.update(params or {})
        if self.oauth_token:
            oparams['oauth_token'] = self.oauth_token

        url = self._get_url(url)
        oparams['oauth_signature'] = self.signature.sign(self.consumer_secret, method, url,
                                                         oauth_token_secret=self.oauth_token_secret, **oparams)

        return await self.sess.request(method, url, params=oparams, headers=headers, **aio_kwargs)

    def close(self):
        self.sess.close()


class SearchClient:
    def __init__(self, user_agent, proxy):
        self.session = aiohttp.ClientSession(headers={'User-Agent': user_agent})
        self.proxy = proxy

    def build_query(self, q):
        pass

    def parse_raw_html(self, h):
        pass

    @staticmethod
    def urls_generator(result):
        yield result

    @staticmethod
    def clean_urls(urls):
        domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        for i in urls:
            is_root_url = any(i.endswith(j) for j in domains)
            is_not_relevant = any(j in i for j in false_urls)
            if i.startswith('http') and not (is_root_url or is_not_relevant):
                yield i

    async def fetch_url(self, url):
        async with self.session.get(url) as response:  # proxy=self.proxy
            return await response.text()

    async def execute_query(self, q):
        """Executes the given search query and returns the result"""
        query_url = self.build_query(q)
        raw = await self.fetch_url(query_url)
        scraped_urls = set()
        try:
            await asyncio.sleep(0)
            for url in self.clean_urls(self.urls_generator(raw)):
                scraped_urls.add(url)
                await asyncio.sleep(0)
        except Exception as e:
            pass
        return scraped_urls


class Producer(Process):
    def __init__(self, port):
        super(Producer, self).__init__()
        self.result_queue = Queue()
        self.outgoing = None
        self.port = port
        self.running = True

    def initialize_work(self):
        return []

    @staticmethod
    def load_query_dictionary(file_name):
        """Loads query topics and corresponding queries from disk"""
        queries = {}
        with open(os.path.join(BASE_DIR, 'lib', 'documents', file_name), 'r') as f:
            lns = f.read().splitlines()
            for l in lns:
                x, y = l.split(':')
                queries[x] = y.split(',')
        return queries

    @staticmethod
    def load_authentications(file_name, query_dictionary):
        """Load api keys seperated by '|' from file"""
        api_keys = {}
        with open(os.path.join(BASE_DIR, 'lib', 'api', file_name), 'r') as f:
            data = f.read().splitlines()
            for i, topic in enumerate(query_dictionary):
                api_keys[topic] = data[i].split('|')
        return api_keys

    @staticmethod
    def run_in_loop(jobs):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*jobs))

    def run(self):
        listener = Listener(('localhost', self.port), authkey=b'veryscrape')
        self.outgoing = listener.accept()
        jobs = self.initialize_work()
        for set_of_jobs in jobs:
            if set_of_jobs:
                Thread(target=self.run_in_loop, args=(set_of_jobs,)).start()
        while self.running:
            self.outgoing.send(self.result_queue.get())
