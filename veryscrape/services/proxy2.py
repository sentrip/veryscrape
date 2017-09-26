import asyncio
import json
from collections import defaultdict
from functools import partial
from queue import Queue
from threading import Thread
from urllib.parse import urlencode

import aiohttp
import aiohttp.web as web

from veryscrape import ExponentialBackOff


# def verify_pair(key, secret):
#     correct = '1318bffaf4fcbb28e517760815023f8ac9ed26a63c99c12060ecef370aa5c4d8' \
#               '58f244ec04360b0b8133369a11fc22c1c9f1a4b2d68d6e37897b714998cf93eb'
#     if sha512(key.encode() + secret.encode()).hexdigest() == correct:
#         return True
#     return False
#
#
# async def handler(queue, request):
#     try:
#         r_json = await request.json()
#     except json.JSONDecodeError:
#         return web.Response(text="Incorrectly formatted json", status=404)
#     try:
#         if len(r_json['apiKey']) != 65:
#             raise KeyError
#         key, secret = r_json['apiKey'].split('-')
#         if len(key) == len(secret):
#             if verify_pair(key, secret):
#                 if all(k in r_json for k in ['content', 'topic', 'source']):
#                     queue.put(Item(r_json['content'], r_json['topic'], r_json['source']))
#                     return web.Response(text="Successfully received", status=200)
#                 else:
#                     return web.Response(text="Invalid json - must contain: [content, topic, source, apiKey]",
#                                         status=400)
#     except (ValueError, KeyError):
#         return web.Response(text="Unauthorized", status=401)
#
#
# async def main(loop, queue):
#     server = web.Server(partial(handler, queue))
#     await loop.create_server(server, '127.0.0.1', 6000)
#     # pause here for very long time by serving HTTP requests and
#     # waiting for keyboard interruption
#     while True:
#         try:
#             await asyncio.sleep(100*3600)
#         except KeyboardInterrupt:
#             server.shutdown()
#             break
#
#
# if __name__ == '__main__':
#     q = Queue()
#
#     def run():
#         policy = asyncio.get_event_loop_policy()
#         policy.set_event_loop(policy.new_event_loop())
#         l = asyncio.get_event_loop()
#         l.run_until_complete(main(l, q))
#
#     def p(q):
#         while True:
#             print(q.get())
#
#     Thread(target=run).start()
#     Thread(target=p, args=(q, )).start()


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


async def random_proxy(**params):
    base = 'https://api.getproxylist.com/proxy?'
    async for _ in ExponentialBackOff(1):
        async with aiohttp.ClientSession() as session:
            async with session.request('GET', base + urlencode(params)) as response:
                try:
                    j = await response.json()
                    return Proxy(**j)
                except aiohttp.client_exceptions.ClientError:
                    pass


async def fetch(q, **params):
    while True:
        i = await random_proxy(**params)
        q.put(i)


class ProxySnatcher(Thread):
    def __init__(self, parent_server):
        super(ProxySnatcher, self).__init__()
        self.n_proxies = 10
        self.parent = parent_server

    async def stream(self):
        while True:
            if len(self.parent.proxies) < self.n_proxies:
                pr = await random_proxy()
                _ = self.parent.seen[pr.ip]
            else:
                await asyncio.sleep(1)

    def run(self):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        l = asyncio.get_event_loop()
        l.run_until_complete(self.stream())


class SServer(web.Server):
    proxies = []
    queue = Queue()
    seen = defaultdict(partial(defaultdict, int))


class T:
    queue = Queue()
    seen = defaultdict(partial(defaultdict, int))


async def handler(parent, request):
    try:
        r_json = await request.json()
    except json.JSONDecodeError:
        return



if __name__ == '__main__':
    lt = T()
    p = ProxySnatcher(lt)
    p.start()
    while True:
        print(lt.queue.get())
