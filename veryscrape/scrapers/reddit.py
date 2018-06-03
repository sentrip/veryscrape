from datetime import datetime
import json

from ..items import ItemGenerator
from ..session import OAuth2Session
from ..scrape import Scraper


class RedditSession(OAuth2Session):
    base_url = 'https://oauth.reddit.com/r/'
    user_agent = 'python:veryscrape:v0.1.0 (by /u/jayjay)'
    persist_user_agent = True


class CommentGen(ItemGenerator):
    removed_comments = {'[deleted]', '[removed]'}

    def process_text(self, text):
        if text[0] in self.removed_comments:
            return None
        return text[0]

    def process_time(self, text):
        return datetime.fromtimestamp(float(text[1]))


class Reddit(Scraper):
    source = 'reddit'
    scrape_every = 600
    item_gen = CommentGen
    session_class = RedditSession

    def __init__(self, key, secret, *, proxy_pool=None):
        super(Reddit, self).__init__(
            key, secret, 'https://www.reddit.com/api/v1/access_token',
            proxy_pool=proxy_pool
        )

    async def get_links(self, query):
        res = await self.client.fetch(
            'GET', '%s/hot.json' % query,
            params={'raw_json': 1, 'limit': 100}
        )
        res = json.loads(res or '[]')
        if isinstance(res, dict) and 'data' in res:
            return [i['data']['id'] for i in res['data']['children']]
        return []

    async def get_comments(self, query, link):
        res = await self.client.fetch(
            'GET', '%s/comments/%s.json' % (query, link),
            params={'raw_json': 1, 'limit': 10000, 'depth': 10}
        )
        res = json.loads(res or '[]')
        if isinstance(res, list):
            return [(c['data']['body'], c['data']['created_utc'])
                    for c in res[1]['data']['children'] if c['kind'] == 't1']
        return []

    async def scrape(self, query, topic='', **kwargs):
        links = await self.get_links(query)
        for link in links:
            comments = await self.get_comments(query, link)
            for comment, timestamp in comments:
                await self.queues[topic].put((comment, str(timestamp)))
