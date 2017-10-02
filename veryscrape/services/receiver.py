import asyncio
from threading import Thread

import aiohttp
import aiohttp.web as web

from veryscrape.services.gui import Controller


class MainServer(web.Server):
    def __init__(self, **kwargs):
        super(MainServer, self).__init__(self.process_request, **kwargs)
        self.queue = asyncio.Queue()
        self.expected_keys = ['article', 'blog', 'reddit', 'twitter', 'stock']
        Thread(target=lambda: Controller(self.queue).mainloop()).start()

    async def process_request(self, request):
        try:
            if request.method != 'POST':
                raise TypeError
            data = await request.read()
            dct = eval(data)
            assert set(dct.keys()) == set(self.expected_keys)
            await self.queue.put(dct)
            return web.Response(text='Success!', status=200)

        except (TypeError, AssertionError):
            return web.Response(text="Incorrectly formatted request", status=404)


async def main_server(address):
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()
    server = MainServer()
    await loop.create_server(server, *address)
    while True:
        try:
            await asyncio.sleep(60)
        except KeyboardInterrupt:
            break

    session.close()
    await server.shutdown()
    loop.close()

if __name__ == '__main__':
    add = '192.168.1.53', 9999
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(main_server(add))
