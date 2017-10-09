import asyncio
import re
import time
from functools import partial

import lxml.html as html
from twingly_search.parser import Parser

from veryscrape.api import BaseScraper, ReadBuffer, Item


class Twitter(BaseScraper):
    base_url = 'https://stream.twitter.com/1.1/'
    proxy_params = {'speed': 30, 'https': 1, 'post': 1}
    # Rate limits
    retry_420 = 60
    snooze_time = 0.25

    def __init__(self, auth):
        self.client, self.secret, self.token, self.token_secret = auth

    def setup(self, query, use_proxy=False):
        params = {'language': 'en', 'track': query}
        setup = partial(self.build_request, 'POST', 'statuses/filter.json', oauth=1, params=params, use_proxy=use_proxy)
        return setup

    async def handle_response(self, resp, topic, queue, stream_for=100000000):
        start = time.time()
        if resp.status == 420:
            await asyncio.sleep(self.retry_420)
        elif resp.status != 200:
            raise ConnectionError('Could not connect to twitter')
        else:
            buffer = ReadBuffer(resp)
            async for status in buffer:
                if time.time() - start >= stream_for:
                    break
                if self.filter(status['text']):
                    item = Item(status['text'], topic, 'twitter')
                    await queue.put(item)
            await asyncio.sleep(self.snooze_time)


class Reddit(BaseScraper):
    base_url = 'https://oauth.reddit.com/r/'
    token_url = 'https://www.reddit.com/api/v1/access_token'
    token_expiry = time.time() - 5
    rate_limit = 60
    user_agent = 'test app'
    persist_user_agent = True

    def __init__(self, auth):
        self.client, self.secret = auth
        self.link = '{}/hot.json?raw_json=1&limit=100'
        self.comment_base = '%s/comments/{}.json?raw_json=1&limit=10000&depth=10'
        self.comment = ''

    def setup(self, query):
        self.comment = self.comment_base % query
        setup = partial(self.build_request, 'GET', self.link.format(query), oauth=2, use_proxy=False)
        return setup

    def setup_comments(self, query):
        setup = partial(self.build_request, 'GET', self.comment.format(query), oauth=2, use_proxy=False)
        return setup

    async def handle_comments(self, resp, topic, queue, **kwargs):
        if resp.status == 403:
            raise ConnectionError('Could not connect to reddit')
        else:
            res = await resp.json()
        comments = []
        try:
            assert isinstance(res, dict)
        except AssertionError:
            comments = res[1]['data']['children']
        for c in comments:
            if c['kind'] == 't1' and self.filter(c['data']['body']):
                item = Item(c['data']['body'], topic, 'reddit')
                await queue.put(item)

    async def handle_response(self, resp, topic, queue, **kwargs):
        resp = await resp.json()
        for i in resp['data']['children']:
            link = i['data']['id']
            await self.scrape(link, topic, queue, setup=self.setup_comments, resp_handler=self.handle_comments)


class Twingly(BaseScraper):
    base_url = "https://api.twingly.com/"
    rate_limit = 60

    def __init__(self, auth):
        self.client = auth
        self.parser = Parser()

    def setup(self, query):
        query_string = "{} lang:en tspan:12h page-size:10000".format(query)
        setup = partial(self.build_request, 'GET', 'blog/search/api/v3/search',
                        params={'q': query_string, 'apiKey': self.client})
        return setup

    async def handle_response(self, resp, topic, queue, **kwargs):
        if resp.status == 401:
            raise ConnectionError('Could not connect to twingly')
        else:
            res = await resp.text()
        result = self.parser.parse(res)
        urls = [post.url for post in result.posts]
        for url in self.clean_urls(urls):
            if self.filter(url):
                item = Item(url, topic, 'blog')
                await queue.put(item)


class Google(BaseScraper):
    base_url = 'https://news.google.com/news/search/section/q'
    proxy_params = {'speed': 50, 'https': 1}
    rate_limit = 120

    def setup(self, query, use_proxy=False):
        setup = partial(self.build_request, 'GET', '{}/{}?hl=en&ned=us'.format(query, query), use_proxy=use_proxy)
        return setup

    async def handle_response(self, resp, topic, queue, **kwargs):
        if resp.status != 200:
            raise ConnectionError('Could not connect to google')
        else:
            res = await resp.text()
        urls = set()
        result = html.fromstring(res)
        for e in result.xpath('//*[@href]'):
            if e.get('href') is not None:
                urls.add(e.get('href'))

        for url in self.clean_urls(urls):
            if self.filter:
                item = Item(url, topic, 'article')
                await queue.put(item)


class Finance(BaseScraper):
    base_url = 'http://www.google.com/'
    proxy_params = {'speed': 50, 'https': 1}

    def setup(self, query, use_proxy=False):
        setup = partial(self.build_request, 'GET', 'finance?', params={'q': query}, use_proxy=use_proxy)
        return setup

    async def handle_response(self, resp, topic, queue, **kwargs):
        if resp.status != 200:
            raise ConnectionError('Could not connect to finance')
        else:
            res = await resp.text()

        stock_price = 0.0
        tmp = re.search(r'id="ref_(.*?)">(.*?)<', res)
        if tmp:
            stock_price = eval(tmp.group(2).replace(',', ''))
        item = Item(stock_price, topic, 'article')
        await queue.put(item)
