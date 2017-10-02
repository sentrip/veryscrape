import asyncio
import heapq
from collections import deque
from urllib.parse import urlencode

import aiohttp
import aiohttp.web as web
from aiohttp.client_exceptions import ClientError


def retry(n=5, wait_factor=2):
    def wrapper(fnc):
        async def inner(*args, **kwargs):
            wait, c = 1, 1
            while c <= n:
                try:
                    return await fnc(*args, **kwargs)
                except:
                    await asyncio.sleep(wait)
                    wait *= wait_factor
                c += 1
        return inner
    return wrapper


class Proxy:
    def __init__(self, **params):
        # Proxy parameters
        self.ip = params.get('ip', None)
        self.port = params.get('port', None)
        self.protocol = params.get('protocol', None)
        self.speed = float(params.get('downloadSpeed', '0.1'))
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


@retry()
async def random_proxy(session, url):
    async with session.get(url) as response:
        try:
            j = await response.json()
            assert j['ip']
            return Proxy(**j)
        except (ClientError, AssertionError, KeyError):
            pass


class ProxyServer(web.Server):
    proxies = []
    used_proxies = deque(maxlen=10000)
    fast_proxies = []

    def __init__(self):
        super(ProxyServer, self).__init__(self.process_request)

    def on_data(self, params):
        for proxy, index in reversed(list(zip(self.proxies, range(len(self.proxies))))):
            good = not params
            if not good:
                for k in params:
                    if k == 'speed':
                        good = good or proxy.speed >= float(params[k])
                    else:
                        good = good or proxy not in self.used_proxies
            if good:
                if proxy in self.fast_proxies:
                    self.fast_proxies.remove(proxy)
                if proxy not in self.used_proxies:
                    self.used_proxies.append(proxy)
                    if len(self.proxies) > 1:
                        return self.proxies.pop(index).full_address
                return proxy.full_address
        else:
            raise TypeError('Couldn\'t find you a proxy')

    async def process_request(self, request):
        try:
            if request.method != 'GET':
                raise TypeError
            params = request.query or {}
            result = self.on_data(params)
            return web.Response(text=result, status=200)
        except TypeError:
            return web.Response(text="Incorrectly formatted request", status=404)


async def proxy_server(address):
    params = {'apiKey': '', 'protocol': 'http'}
    base = 'https://api.getproxylist.com/proxy?' + urlencode(params)
    loop = asyncio.get_event_loop()

    session = aiohttp.ClientSession()
    server = ProxyServer()
    await loop.create_server(server, *address)

    proxies_required = 1000
    concurrent_requests = 20

    while True:
        try:
            if len(server.proxies) - len(server.used_proxies) <= proxies_required or len(server.fast_proxies) < int(proxies_required / 5):
                new_proxies = await asyncio.gather(*[random_proxy(session, base) for _ in range(concurrent_requests)])
                for proxy in new_proxies:
                    if proxy is not None:
                        heapq.heappush(server.proxies, proxy)
                        if proxy.speed > 100:
                            server.fast_proxies.append(proxy)
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            break

    session.close()
    await server.shutdown()
    loop.close()


def hammer():
    import aiohttp
    import time

    async def run():
        async with aiohttp.ClientSession() as sess:
            c = 0
            while True:
                async with sess.get('http://192.168.0.103:9999') as response:
                    t = await response.text()
                    if c % 50 == 0: print(t)
                    c += 1
                    assert t.startswith('http') or t.startswith('Incorrect'), t

    async def h():
        time.sleep(3)
        n = 256
        while True:
            await asyncio.gather(*[run() for _ in range(n)])
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(h())

if __name__ == '__main__':
    #from multiprocessing import Process; Process(target=hammer).start()
    add = '192.168.0.100', 9999
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(proxy_server(add))
