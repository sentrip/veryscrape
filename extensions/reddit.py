import asyncio
import time
from collections import defaultdict
from contextlib import suppress
from urllib.parse import urlencode

import aiohttp

from base import Item, AsyncStream


class CommentStream(AsyncStream):
    BASE = 'https://oauth.reddit.com/r/'
    TOKEN_URL = 'https://www.reddit.com/api/v1/access_token'

    def __init__(self, auth, topic, queries, queue):
        self.auth = auth
        self.topic = topic
        self.queries = queries
        self.queue = queue

        self.links = {q: {} for q in queries}
        self.last_ids = {q: defaultdict(int) for q in queries}
        self.last_id = {q: None for q in queries}
        self.seen = {q: set() for q in queries}
        self.infinite_queries = iter(self.queries)

        self.clock = time.time()
        self.session = None
        self.token = None
        self.expiry_time = 0
        self.rate_limit = 60

    async def new_session(self, token=None):
        if self.session:
            self.session.close()
        head = {'user-agent': 'test app'}
        if not token:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(*self.auth), headers=head) as client:
                async with client.post(self.TOKEN_URL, data={'grant_type': 'client_credentials'}) as response:
                    j = await response.json()
                    self.token = j['access_token']
                    head.update({'Authorization': 'bearer ' + self.token})
        else:
            head.update({'Authorization': 'bearer ' + token})
        self.session = aiohttp.ClientSession(headers=head)
        self.expiry_time = time.time() + 3598

    async def fetch_json(self, url):
        if self.rate_limit <= 0:
            await asyncio.sleep(1)
        self.rate_limit -= 1
        try:
            async with self.session.get(url) as response:
                data = await response.json()
            return data
        except Exception as e:
            print('Reddit', repr(e))
            await self.new_session(self.token)
            return {}

    async def get_comments(self, query,**params):
        try:
            link_url = self.BASE + '{}/hot.json?{}&raw_json=1&limit=30'.format(query, urlencode(params))
            comment_url = self.BASE + '%s/comments/{}.json?%s&raw_json=1&limit=10000&depth=10' % (query, urlencode(params))
            j = await self.fetch_json(link_url)
            links = {i['data']['id']: i['data']['created_utc'] for i in j['data']['children']}
            last_id = j['data']['after']
            for link in links:
                with suppress(KeyError):
                    j = await self.fetch_json(comment_url.format(link))
                    for c in j[1]['data']['children']:
                        if c['kind'] == 't1':
                            if c['data']['id'] not in self.seen[query]:
                                self.queue.put(Item(c['data']['body_html'], self.topic, 'reddit'))
                                self.seen[query].add(c['data']['id'])
            return last_id
        except KeyError:
            return None

    async def __anext__(self):
        since = time.time() - self.clock
        if since >= 1:
            self.rate_limit += int(since)
            self.clock += int(since)

        if self.expiry_time - time.time() <= 0:
            if self.session:
                self.session.close()
            await self.new_session()

        try:
            q = next(self.infinite_queries)
        except StopIteration:
            await asyncio.sleep(15)
            self.infinite_queries = iter(self.queries)
            q = next(self.infinite_queries)

        for link, time_created in list(self.links[q].items()):
            if time.time() - time_created >= 2 * 24 * 3600:
                del self.links[q][link]

        params = {}
        if self.last_id[q]:
            params['after'] = self.last_id[q]
        if self.last_ids[q][self.last_id[q]]:
            params['count'] = self.last_ids[q][self.last_id[q]]
        self.last_id[q] = await self.get_comments(q, **params)
        return
