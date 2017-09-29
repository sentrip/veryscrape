import asyncio
import time
from collections import defaultdict
from urllib.parse import urlencode

from veryscrape import SearchClient, async_run_forever


class Reddit(SearchClient):
    base_url = 'https://oauth.reddit.com/r/'
    token_url = 'https://www.reddit.com/api/v1/access_token'
    token_expiry = time.time()
    rate_limit = 60
    user_agent = 'test app'

    # Reddit api paths
    link_url = '{}/hot.json?raw_json=1&limit=100{}'
    comment_url = '{}/comments/{}.json?raw_json=1&limit=10000&depth=10{}'

    seen_comments = set()
    links = defaultdict(time.time)
    last_id = ''

    def __init__(self, auth):
        super(Reddit, self).__init__()
        self.client, self.secret = auth

    async def get_links(self, track, **params):
        url = self.link_url.format(track, ('&' + urlencode(params)) if params else '')
        resp = await self.request('GET', url, oauth=2, return_json=True)
        self.last_id = resp['data']['after'] or ''
        links = {i['data']['id']: i['data']['created_utc'] for i in resp['data']['children']}
        return links

    async def get_comments(self, track, link, **params):
        url = self.comment_url.format(track, link, ('&' + urlencode(params)) if params else '')
        resp = await self.request('GET', url, oauth=2, return_json=True)
        comments = resp[1]['data']['children']
        return comments

    async def send_comments(self, comments, topic):
        for c in comments:
            if c['kind'] == 't1' and c['data']['id'] not in self.seen_comments:
                self.seen_comments.add(c['data']['id'])
                await self.send_item(c['data']['body_html'], topic, 'reddit')

    async def comment_stream(self, track=None, topic=None, duration=10800):
        start_time = time.time()
        while True:
            params = {'after': self.last_id} if self.last_id else {}

            links = await self.get_links(track, **params)
            self.links.update(links)

            for link, time_created in self.links.items():
                if time.time() - time_created >= 2 * 24 * 3600:
                    del self.links[link]

            for link in self.links:
                comments = await self.get_comments(track, link, **params)
                await self.send_comments(comments, topic)

            if time.time() - start_time >= duration:
                break
            else:
                await asyncio.sleep(30)

    @async_run_forever
    async def stream(self, track=None, topic=None, duration=10800):
        await self.comment_stream(track, topic, duration)
