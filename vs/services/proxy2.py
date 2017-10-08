import asyncio
import heapq
import random

import aiohttp
import aiohttp.web as web


class ProxyList:
    def __init__(self):
        self.proxies = []
        self.used_proxies = set()
        self.n_fast_proxies = 0
        self.query_map = {'speed': 'downloadSpeed', 'https': 'allowsHttps',
                          'post': 'allowsPost', 'user_agent': 'allowsCustomUserAgent'}

    @staticmethod
    def full_address(proxy_dict):
        http = 'https' if proxy_dict['allowsHttps'] else 'http'
        return '{}://{}:{}'.format(http, proxy_dict['ip'], proxy_dict['port'])

    def add(self, proxy_dict):
        if proxy_dict['ip'] not in self.used_proxies:
            self.used_proxies.add(proxy_dict['ip'])
            speed = float(proxy_dict['downloadSpeed'])
            proxy_dict['downloadSpeed'] = speed
            if speed >= 100:
                self.n_fast_proxies += 1
            heapq.heappush(self.proxies, (1 / speed + random.random() / 10, proxy_dict))

    def _pop(self, ind=0):
        if len(self.proxies) > 1:
            p = self.proxies.pop(ind)[1]
            if p['downloadSpeed'] > 100:
                self.n_fast_proxies -= 1
            return p
        else:
            return self.proxies[0][1]

    def pop(self, **proxy_kwargs):
        kwargs = {(i if i not in self.query_map.keys() else self.query_map[i]): v for i, v in proxy_kwargs.items()}
        speed = float(kwargs.pop('downloadSpeed', 0.))
        for ind, (_, proxy) in enumerate(self.proxies):
            if proxy['downloadSpeed'] >= speed:
                for k in kwargs:
                    try:
                        if not proxy[k]:
                            break
                    except KeyError:
                        continue
                else:
                    return self.full_address(self._pop(ind))

        else:
            return self.full_address(self._pop())


async def get(sess, key):
    pp = {'apiKey': key, 'protocol': 'http', 'anonymity': 'high anonymity'}
    try:
        async with sess.get('https://api.getproxylist.com/proxy', params=pp) as resp:
            return await resp.json()
    except:
        pass


class ProxyServer(web.Server):
    def __init__(self):
        super(ProxyServer, self).__init__(self.process_request)
        self.proxy_list = ProxyList()

    async def process_request(self, request):
        try:
            if request.method != 'GET':
                raise TypeError
            params = request.query or {}
            result = self.proxy_list.pop(**params)
            return web.Response(text=result, status=200)
        except TypeError:
            return web.Response(text="Incorrectly formatted request", status=404)

    async def fetch_proxies(self, proxies_required=1000, concurrent_requests=10):
        async with aiohttp.ClientSession() as session:
            async with session.get('http://192.168.0.100:1111', params={'type': 'proxy'}) as raw:
                resp = await raw.text()
            api_key = eval(resp)['auth'][0][0]
            while True:
                if len(self.proxy_list.proxies) < proxies_required \
                        or self.proxy_list.n_fast_proxies < int(proxies_required / 5):
                    results = await asyncio.gather(*[get(session, api_key) for _ in range(concurrent_requests)])
                    for result in results:
                        if isinstance(result, dict):
                            self.proxy_list.add(result)
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(5)
                await asyncio.sleep(0)

async def proxy_server(address, proxies_required=1000, concurrent_requests=10):
    loop = asyncio.get_event_loop()
    server = ProxyServer()
    await loop.create_server(server, *address)
    await server.fetch_proxies(proxies_required, concurrent_requests)

if __name__ == '__main__':
    add = '192.168.0.100', 9999
    pr = 1000
    cr = 10
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(proxy_server(add, pr, cr))
