import asyncio
import json
import re

import aiohttp.web as web


def schema(l):
    s = '[0-9a-zA-Z]{%s}-' * len(l)
    r = s % tuple(i for i in l)
    return r[:-1]


class APIServer(web.Server):
    def __init__(self):
        super(APIServer, self).__init__(self.process_request)

    def on_data(self, params):
        source = params.get('type', None)
        if source is None:
            raise TypeError
        else:
            with open('data/{}.txt'.format(source)) as f:
                data = f.read().splitlines()
                auth = [i.split('|') for i in data]
            return json.dumps({'auth': auth})

    def on_post(self, params, data):
        try:
            t = params['type']
            auth = data['auth']
            write = False
            if t == 'twingly':
                search = re.compile(schema([8, 4, 4, 4, 12]))
                if len(re.findall(search, auth)) != 1:
                    raise KeyError
                else:
                    write = True

            if write:
                with open('data/{}.txt'.format(t), 'a') as f:
                    f.write('{}\n'.format(auth))
                return 'Success', 200
            else:
                return 'Incorrect key format', 401
        except KeyError:
            return 'Failed!', 401

    async def process_request(self, request):
        try:
            params = request.query or {}
            if request.method == 'GET':
                result = self.on_data(params)
                return web.Response(text=result, status=200)
            elif request.method == 'POST':
                data = await request.post()
                result, code = self.on_post(params, data)
                return web.Response(text=result, status=code)
            else:
                raise TypeError
        except TypeError:
            return web.Response(text="Incorrectly formatted request", status=404)


async def api_server(address):
    loop = asyncio.get_event_loop()

    server = APIServer()
    await loop.create_server(server, *address)

    while True:
        try:
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            break

    await server.shutdown()
    loop.close()

if __name__ == '__main__':
    add = '192.168.0.100', 1111
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(api_server(add))
