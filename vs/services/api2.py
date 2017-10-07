import asyncio
import os
import re

import aiohttp.web as web


class FileKeeper:
    def __init__(self):
        self.base = r'C:\Users\Djordje\Desktop\lib\d'
        self.data = {}
        self.schemas = {}
        self.load_files()
        for i in self.data:
            self.schemas[i] = self.schema([8, 4, 4, 4, 12])

    def load_files(self):
        fs = os.listdir(self.base)
        for f in fs:
            with open(os.path.join(self.base, f)) as fn:
                if any(i in f for i in ['topics', 'subreddits']):
                    data = {}
                    for i in fn.read().splitlines():
                        c, qs = i.split(':')
                        data[c] = qs.split(',')
                else:
                    data = [i.split('|') for i in fn.read().splitlines()]
                    if len(data) == 1:
                        data = '"{}"'.format(data[0][0])
                self.data[f.replace('.txt', '')] = data

    @staticmethod
    def schema(l):
        s = '[0-9a-zA-Z]{%s}-' * len(l)
        r = s % tuple(i for i in l)
        return r[:-1]

    def __getitem__(self, item):
        return self.data[item]

    def update(self, key, source):
        t = 'w' if source == 'twingly' else 'a'
        search = re.compile(self.schemas[source])
        if len(re.findall(search, key)) != 1:
            raise KeyError
        else:
            with open(os.path.join(self.base, source + '.txt'), t) as f:
                f.write(key + '\n')


class APIServer(web.Server):
    def __init__(self):
        super(APIServer, self).__init__(self.process_request)
        self.files = FileKeeper()

    def on_data(self, params):
        source = params.get('q', None)
        if source is None:
            raise TypeError
        else:
            return str(self.files[source])

    def on_post(self, params, data):
        try:
            t = params['q']
            auth = data['auth']
            self.files.update(auth, t)
            return 'Success', 200
        except KeyError:
            return 'Failed!', 401

    async def process_request(self, request):
        params = request.query or {}
        if request.method == 'GET':
            result = self.on_data(params)
            return web.Response(text=result, status=200)
        elif request.method == 'POST':
            data = await request.post()
            result, code = self.on_post(params, data)
            return web.Response(text=result, status=code)


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
    add = '127.0.0.1', 1111
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(api_server(add))
