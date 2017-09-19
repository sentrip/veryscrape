import asyncio
import time
from hashlib import sha1
from multiprocessing import Process
from multiprocessing.connection import Listener
from random import SystemRandom

import aioauth_client
import aiohttp

random = SystemRandom().random
BASE_DIR = "/home/djordje/Sentrip/"
#BASE_DIR = "C:/users/djordje/desktop"


def load_query_dictionary(file_name):
    """Loads query topics and corresponding queries from disk"""
    queries = {}
    with open(file_name, 'r') as f:
        lns = f.read().splitlines()
        for l in lns:
            x, y = l.split(':')
            queries[x] = y.split(',')
    return queries


def load_authentications(file_name, query_dictionary):
    """Load api keys seperated by '|' from file"""
    api_keys = {}
    with open(file_name, 'r') as f:
        data = f.read().splitlines()
        for i, topic in enumerate(query_dictionary):
            api_keys[topic] = data[i].split('|')
    return api_keys


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


class Producer(Process):
    def __init__(self, port):
        super(Producer, self).__init__()
        self.outgoing = None
        self.listener = Listener(('localhost', port), authkey=b'veryscrape')
        self.running = True

    def initialize_work(self):
        return []

    async def send(self, item):
        self.outgoing.send(item)
        await asyncio.sleep(0)

    async def serve(self, jobs):
        while self.running:
            await asyncio.gather(*[asyncio.ensure_future(j) for j in jobs])

    def run(self):
        self.outgoing = self.listener.accept()
        jobs = self.initialize_work()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.serve(jobs))
