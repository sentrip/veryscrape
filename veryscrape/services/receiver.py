import asyncio
import re
from collections import namedtuple
from multiprocessing import Queue

import aiohttp
import aiohttp.web as web

Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


class MainServer(web.Server):
    def __init__(self, **kwargs):
        super(MainServer, self).__init__(self.process_request, **kwargs)
        self.queue = Queue()

    async def process_request(self, request):
        try:
            r_json = await request.json()
            result = Item(**eval(r_json))
            self.queue.put(result)
            return web.Response(text='Success!', status=200)

        except TypeError:
            return web.Response(text="Incorrectly formatted request", status=404)


async def main_server(address):
    loop = asyncio.get_event_loop()

    session = aiohttp.ClientSession()
    server = MainServer()
    await loop.create_server(server, *address)
    c = 0
    while True:
        try:
            if not server.queue.empty():
                _ = server.queue.get()
                if c % 100 == 0:
                    print(c)
                c += 1
            else:
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            break

    session.close()
    await server.shutdown()
    loop.close()



import json, time, multiprocessing


async def fetch(s, js):
    async with s.get('http://127.0.0.1:9999', json=js) as response:
        await response.text()

async def lp():
    js = json.dumps({'topic': 'AAPL', 'source': 'twitter', 'content': 'hello'})
    s = aiohttp.ClientSession()
    while True:
        try:
            asyncio.ensure_future(fetch(s, js))
            await asyncio.sleep(0)
        except KeyboardInterrupt:
            break
    s.close()


def run():
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(lp())


def hammer(n_processes=4):
    time.sleep(5)
    for _ in range(n_processes):
        multiprocessing.Process(target=run).start()

if __name__ == '__main__':
    add = '127.0.0.1', 9999
    #hammer(4)
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(main_server(add))
