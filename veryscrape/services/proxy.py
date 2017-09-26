import asyncio
import heapq
import json
from collections import deque
from hashlib import sha512
from urllib.parse import urlencode

import aiohttp
import aiohttp.web as web
from aiohttp.client_exceptions import ClientError


class ExponentialBackOff:
    def __init__(self, ratio=2):
        self.ratio = ratio
        self.count = 0
        self.retry_time = 1

    def reset(self):
        self.count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.count:
            await asyncio.sleep(self.retry_time)
            self.retry_time *= self.ratio
        self.count += 1
        return self.count


class Proxy:
    def __init__(self, **params):
        # Proxy parameters
        self.ip = params.get('ip', None)
        self.port = params.get('port', None)
        self.protocol = params.get('protocol', None)
        self.speed = float(params.get('downloadSpeed', 0.1) or 0.1)
        # Proxy capability
        self.https = params.get('allowsHttps', False)
        self.post = params.get('allowsPost', False)
        self.cookies = params.get('allowsCookies', False)
        self.custom_headers = params.get('allowsCustomHeaders', False)
        self.user_agent = params.get('allowsUserAgentHeader', False)
        self.referrer = params.get('allowsRefererHeader', False)
        # Location and quality
        self.tested = params.get('lastTested', None)
        self.secs_to_first_byte = params.get('secondsToFirstByte', None)
        self.connect_time = params.get('connectTime', None)
        # Composite values
        self.address = '{}:{}'.format(self.ip, self.port)
        self.full_address = '{}://{}'.format(self.protocol, self.address)
        self.proxy_dict = {'https' if self.https else 'http': self.full_address}

    def __lt__(self, other):
        return self.speed < other.speed

    def __gt__(self, other):
        return self.speed > other.speed

    def __eq__(self, other):
        return self.ip == other.ip

    def __repr__(self):
        return 'Proxy({:4s}, {:.1f})'.format(self.protocol, self.speed)


class APIServer(web.Server):
    def __init__(self, **kwargs):
        super(APIServer, self).__init__(self.process_request, **kwargs)
        self.key = '1318bffaf4fcbb28e517760815023f8ac9ed26a63c99c12060ecef370aa5c4d8' \
                   '58f244ec04360b0b8133369a11fc22c1c9f1a4b2d68d6e37897b714998cf93eb'

    def on_data(self, data):
        return json.dumps(data)

    def verify_key(self, key):
        client, secret = key.split('-')
        m = sha512(client.encode() + secret.encode())
        if m.hexdigest() == self.key:
            return True
        else:
            return False

    async def process_request(self, request):
        try:
            r_json = eval(await request.json())
            try:
                if self.verify_key(r_json['apiKey']):
                    del r_json['apiKey']
                    result = self.on_data(r_json)
                    return web.Response(text=result, status=200)
                raise KeyError
            except KeyError:
                return web.Response(text="Unauthorized", status=401)
        except TypeError:
            return web.Response(text="Incorrectly formatted request", status=404)


async def random_proxy(session, url):
    waiter = ExponentialBackOff(1)
    async for _ in waiter:
        async with session.get(url) as response:
            try:
                j = await response.json()
                return Proxy(**j)
            except ClientError:
                pass


class ProxyServer(APIServer):
    proxies = []
    used_proxies = deque(maxlen=10000)
    fast_proxies = []

    def find_matching_proxy(self, **params):
        for proxy, index in reversed(list(zip(self.proxies, range(len(self.proxies))))):
            good = False
            for k, v in params.items():
                if k == 'speed':
                    good = good or proxy.speed >= v
                else:
                    good = good or proxy not in self.used_proxies
            if good:
                if len(self.proxies) > 1:
                    return self.proxies.pop(index)
                else:
                    return proxy
        else:
            return None

    def on_data(self, data):
        proxy = self.find_matching_proxy(**data)
        if proxy in self.fast_proxies:
            self.fast_proxies.remove(proxy)
        if proxy is None:
            return 'No matching proxies were found, please try other query combinations'
        else:
            self.used_proxies.append(proxy)
            return proxy.full_address


async def proxy_server(address):
    params = {'apiKey': '3c6527a27448d2873e2e4cd0114c202bfd0ff58f', 'anonymity': 'high anonymity', 'protocol': 'http'}
    base = 'https://api.getproxylist.com/proxy?' + urlencode(params)
    loop = asyncio.get_event_loop()

    session = aiohttp.ClientSession()
    server = ProxyServer()
    await loop.create_server(server, *address)

    proxies_required = 500
    concurrent_requests = 20

    while True:
        try:
            if len(server.proxies) - len(server.used_proxies) <= proxies_required or len(server.fast_proxies) < int(proxies_required / 5):
                new_proxies = await asyncio.gather(*[random_proxy(session, base) for _ in range(concurrent_requests)])
                for proxy in new_proxies:
                    heapq.heappush(server.proxies, proxy)
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            break

    session.close()
    await server.shutdown()
    loop.close()

if __name__ == '__main__':
    add = '127.0.0.1', 9999
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(proxy_server(add))
