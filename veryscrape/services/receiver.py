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
            if request.method != 'POST':
                raise TypeError
            data = await request.post()
            self.queue.put(Item(**data))
            return web.Response(text='Success!', status=200)

        except TypeError:
            return web.Response(text="Incorrectly formatted request", status=404)


async def main_server(address):
    loop = asyncio.get_event_loop()

    session = aiohttp.ClientSession()
    server = MainServer()
    await loop.create_server(server, *address)
    c = 1
    while True:
        try:
            if not server.queue.empty():
                _ = server.queue.get()
                print(c, _)
                c += 1
            else:
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            break

    session.close()
    await server.shutdown()
    loop.close()

if __name__ == '__main__':
    add = '192.168.1.53', 9999
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(main_server(add))
