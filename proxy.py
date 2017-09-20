# Custom thread to fetch and sort proxies for continuous use
import asyncio
import heapq
import json
import os
from collections import deque
from threading import Thread
from urllib.parse import urlencode

import aiohttp

from base import BASE_DIR


class ProxySnatcher(Thread):
    """'allowsPost', 'downloadSpeed', 'allowsCookies', 'country',
    'allowsRefererHeader', 'secondsToFirstByte', '_links', 'ip',
    'anonymity', 'connectTime', 'protocol', 'allowsCustomHeaders',
    'uptime', 'port', 'allowsHttps', 'lastTested', 'allowsUserAgentHeader'"""
    def __init__(self, proxies_required=10, **params):
        super(ProxySnatcher, self).__init__()
        self.proxies_required = proxies_required
        with open(os.path.join(BASE_DIR, 'lib', 'api', 'proxy.txt'), 'r') as f:
            key = f.read()
        params.update({'apiKey': key})
        self.url = 'https://api.getproxylist.com/proxy?anonymity=high%20anonymity&' + urlencode(params)
        self.proxies = {'reddit': [], 'twitter': [], 'google': [], 'twingly': []}
        self.seen_proxies = deque(maxlen=proxies_required*10)
        self.running = True

    async def random(self, proxy_type):
        """Returns random proxy that was not used recently"""
        while len(self.proxies[proxy_type]) == 0:
            await asyncio.sleep(0.001)
        random_proxy = heapq.heappop(self.proxies[proxy_type])[1]
        h = 'https' if random_proxy['allowsHttps'] else 'http'
        return {h: '{}://{}'.format(random_proxy.protocol, random_proxy.address)}

    async def fetch_proxies(self):
        """Fetches proxies from api and pushes onto heap"""
        while self.running:
            while any(len(self.proxies[t]) < self.proxies_required * 2 for t in self.proxies):
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url) as response:
                        data = await response.text()
                        j = json.loads(data)
                        if j['ip'] not in self.seen_proxies:
                            self.seen_proxies.append(j['ip'])
                            for t in self.proxies:
                                heapq.heappush(self.proxies[t], (1/float(j['downloadSpeed']), j))
            await asyncio.sleep(0.1*10)
            print(*[len(i) for i in self.proxies.values()])

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.fetch_proxies())
