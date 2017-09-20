# Custom thread to fetch and sort proxies for continuous use
import asyncio
import heapq
import json
import os
from collections import deque
from threading import Thread
from time import sleep
from urllib.parse import urlencode

import aiohttp

from base import BASE_DIR


class Proxy:
    def __init__(self, **params):
        # Proxy parameters
        self.ip = params.get('ip', None)
        self.port = params.get('port', None)
        self.protocol = params.get('protocol', None)
        self.anonymity = params.get('anonymity', None)
        self.downloadSpeed = float(params.get('downloadSpeed', 0.001))
        # Proxy capability
        self.allowsHttps = params.get('allowsHttps', False)
        self.allowsPost = params.get('allowsPost', False)
        self.allowsCookies = params.get('allowsCookies', False)
        self.allowsCustomHeaders = params.get('allowsCustomHeaders', False)
        self.allowsUserAgentHeader = params.get('ip', False)
        self.allowsRefererHeader = params.get('allowsRefererHeader', False)
        # Location and quality
        self.country = params.get('country', None)
        self.lastTested = params.get('lastTested', None)
        self.secondsToFirstByte = params.get('secondsToFirstByte', None)
        self.connectTime = params.get('connectTime', None)
        self.uptime = params.get('uptime', None)
        # Composite values
        self.address = '{}:{}'.format(self.ip, self.port)
        self.full_address = '{}://{}'.format(self.protocol, self.address)
        self.proxy_dict = {'https' if self.allowsHttps else 'http': self.full_address}

    def __lt__(self, other):
        return self.downloadSpeed < other.downloadSpeed

    def __gt__(self, other):
        return self.downloadSpeed < other.downloadSpeed

    def __eq__(self, other):
        return self.ip == other.ip

    def __repr__(self):
        return 'Proxy({:7s}, {:.1f})'.format(self.protocol, self.downloadSpeed)


class ProxySnatcher(Thread):
    def __init__(self, proxies_required=10, **params):
        super(ProxySnatcher, self).__init__()
        self.proxies_required = proxies_required
        with open(os.path.join(BASE_DIR, 'lib', 'api', 'proxy.txt'), 'r') as f:
            key = f.read()
        params.update({'apiKey': key})
        self.url = 'https://api.getproxylist.com/proxy?anonymity=high%20anonymity&' + urlencode(params)
        self.proxies = {'reddit': [], 'twitter': [], 'article': [], 'blog': []}
        self.seen_proxies = deque(maxlen=proxies_required*10)
        self.running = True

    def random(self, proxy_type, return_dict=False):
        """Returns random proxy that was not used recently"""
        while len(self.proxies[proxy_type]) == 0:
            sleep(0.01)
        random_proxy = heapq.heappop(self.proxies[proxy_type])
        return random_proxy.proxy_dict if return_dict else random_proxy.full_address

    async def fetch_proxies(self):
        """Fetches proxies from api and pushes onto heap"""
        while self.running:
            while any(len(self.proxies[t]) < self.proxies_required * 2 for t in self.proxies):
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url) as response:
                        data = await response.text()
                        proxy = Proxy(**json.loads(data))
                        for t in self.proxies:
                            if proxy not in self.proxies[t]:
                                heapq.heappush(self.proxies[t], proxy)
            await asyncio.sleep(0.1)

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.fetch_proxies())
