# Custom thread to fetch and sort proxies for continuous use
import os
from urllib.parse import urlencode

import aiohttp
import requests

from src.base import BASE_DIR, ExponentialBackOff


def get_key():
    with open(os.path.join(BASE_DIR, 'lib', 'api', 'proxy.txt'), 'r') as f:
        return f.read()


async def random_proxy(**params):
    params.update({'apiKey': get_key()})
    url = 'https://api.getproxylist.com/proxy?anonymity=high%20anonymity&' + urlencode(params)
    retry_counter = ExponentialBackOff(1)
    async for _ in retry_counter:
        async with aiohttp.ClientSession() as session:
            async with session.request('GET', url) as response:
                try:
                    j = await response.json()
                    return '{}://{}:{}'.format(j['protocol'], j['ip'], j['port'])
                except aiohttp.client_exceptions.ClientError:
                    pass
                except Exception as e:
                    print('Proxy', repr(e))


def random_proxy_sync(**params):
    params.update({'apiKey': get_key()})
    while True:
        url = 'https://api.getproxylist.com/proxy?anonymity=high%20anonymity&' + urlencode(params)
        try:
            j = requests.get(url).json()
            return {'https' if j['allowsHttps'] else 'http': '{}://{}:{}'.format(j['protocol'], j['ip'], j['port'])}
        except requests.exceptions.ConnectionError:
            pass
        except Exception as e:
            print('Proxy', repr(e))


# import asyncio
# import heapq
# import json
# from collections import deque
# from threading import Thread
# from time import sleep
# class Proxy:
#     def __init__(self, **params):
#         # Proxy parameters
#         self.ip = params.get('ip', None)
#         self.port = params.get('port', None)
#         self.protocol = params.get('protocol', None)
#         self.anonymity = params.get('anonymity', None)
#         self.downloadSpeed = float(params.get('downloadSpeed', 0.001))
#         # Proxy capability
#         self.allowsHttps = params.get('allowsHttps', False)
#         self.allowsPost = params.get('allowsPost', False)
#         self.allowsCookies = params.get('allowsCookies', False)
#         self.allowsCustomHeaders = params.get('allowsCustomHeaders', False)
#         self.allowsUserAgentHeader = params.get('ip', False)
#         self.allowsRefererHeader = params.get('allowsRefererHeader', False)
#         # Location and quality
#         self.country = params.get('country', None)
#         self.lastTested = params.get('lastTested', None)
#         self.secondsToFirstByte = params.get('secondsToFirstByte', None)
#         self.connectTime = params.get('connectTime', None)
#         self.uptime = params.get('uptime', None)
#         # Composite values
#         self.address = '{}:{}'.format(self.ip, self.port)
#         self.full_address = '{}://{}'.format(self.protocol, self.address)
#         self.proxy_dict = {'https' if self.allowsHttps else 'http': self.full_address}
#
#     def __lt__(self, other):
#         return self.downloadSpeed < other.downloadSpeed
#
#     def __gt__(self, other):
#         return self.downloadSpeed < other.downloadSpeed
#
#     def __eq__(self, other):
#         return self.ip == other.ip
#
#     def __repr__(self):
#         return 'Proxy({:7s}, {:.1f})'.format(self.protocol, self.downloadSpeed)
#
#
# class ProxySnatcher(Thread):
#     def __init__(self, proxies_required=10, **params):
#         super(ProxySnatcher, self).__init__()
#         self.proxies_required = proxies_required
#         with open(os.path.join(BASE_DIR, 'lib', 'api', 'proxy.txt'), 'r') as f:
#             key = f.read()
#         params.update({'apiKey': key})
#         self.url = 'https://api.getproxylist.com/proxy?anonymity=high%20anonymity&' + urlencode(params)
#         self.proxies = {'article': [], 'stock': [], 'twitter': []}
#         self.seen_proxies = deque(maxlen=proxies_required*10)
#         self.session = None
#         self.running = True
#
#     def random(self, proxy_type, return_dict=False):
#         """Returns random proxy that was not used recently"""
#         while len(self.proxies[proxy_type]) == 0:
#             sleep(0.01)
#         p = heapq.heappop(self.proxies[proxy_type])
#         return p.proxy_dict if return_dict else p.full_address
#
#     async def random_async(self, proxy_type, return_dict=False):
#         """Returns random proxy that was not used recently"""
#         while len(self.proxies[proxy_type]) == 0:
#             await asyncio.sleep(1)
#         p = heapq.heappop(self.proxies[proxy_type])
#         return p.proxy_dict if return_dict else p.full_address
#
#     def wait_for_proxies(self):
#         print('Now waiting to acquire proxies...')
#         while len(self.proxies['article']) < self.proxies_required * 2:
#             sleep(1)
#             print('Currently have {}/{}'.format(len(self.proxies['article']), self.proxies_required*2))
#         print('Proxies acquired, now initializing streams...')
#
#     async def fetch_single(self):
#         try:
#             async with self.session.get(self.url) as response:
#                 data = await response.text()
#         except:
#             data = {}
#             self.session.close()
#             await asyncio.sleep(0.5)
#             self.session = aiohttp.ClientSession()
#         if data:
#             seen = True
#             proxy = Proxy(**json.loads(data))
#             for t in self.proxies:
#                 if proxy not in self.seen_proxies:
#                     seen = False
#                     heapq.heappush(self.proxies[t], proxy)
#             if not seen:
#                 self.seen_proxies.append(proxy)
#
#     async def fetch_proxies(self):
#         """Fetches proxies from api and pushes onto heap"""
#         self.session = aiohttp.ClientSession()
#         try:
#             while self.running:
#                 if any(len(self.proxies[t]) <= self.proxies_required * 2 for t in self.proxies):
#                     n = max(1, min(15, 2 * self.proxies_required - min(*[len(self.proxies[t]) for t in self.proxies])))
#                     await asyncio.gather(*[self.fetch_single() for _ in range(n)])
#         finally:
#             if not self.session.closed:
#                 self.session.close()
#
#     def run(self):
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(self.fetch_proxies())
