import asyncio
import logging
import re
import time

import aiohttp
import lxml.html as html
from api import BaseScraper, ReadBuffer, Item
from retrying import retry
from twingly_search.parser import Parser

log = logging.getLogger(__name__)


class Twitter(BaseScraper):
    base_url = 'https://stream.twitter.com/1.1/'
    proxy_params = {'speed': 30, 'https': 1, 'post': 1, 'anonymity': 2}
    # Rate limits
    retry_420 = 60
    snooze_time = 0.25

    def __init__(self, auth):
        self.client, self.secret, self.token, self.token_secret = auth

    def setup(self, query):
        params = {'language': 'en', 'track': query}
        # setup = partial(self.build_request, 'POST', 'statuses/filter.json', oauth=1, params=params)
        return self.build_request('POST', 'statuses/filter.json', oauth=1, params=params)

    async def handle_response(self, resp, topic, queue, stream_for=100000000, **kwargs):
        start = time.time()
        if resp.status == 420:
            await asyncio.sleep(self.retry_420)
        elif resp.status != 200:
            raise ConnectionError('Could not connect to twitter: {}'.format(resp.status))
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
        # setup = partial(self.build_request, 'GET', self.link.format(query), oauth=2)
        return self.build_request('GET', self.link.format(query), oauth=2)

    def setup_comments(self, query):
        # setup = partial(self.build_request, 'GET', self.comment.format(query), oauth=2)
        return self.build_request('GET', self.comment.format(query), oauth=2)

    async def handle_comments(self, resp, topic, queue, **kwargs):
        if resp.status == 403:
            raise ConnectionError('Could not connect to reddit: {}'.format(resp.status))
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
            try:
                await self.scrape(link, topic, queue, setup=self.setup_comments, resp_handler=self.handle_comments)
            except aiohttp.ClientError:
                await asyncio.sleep(0)


class Google(BaseScraper):
    base_url = 'https://news.google.com/news/search/section/q/'
    proxy_params = {'speed': 50, 'https': 1, 'anonymity': 2}
    rate_limit = 120

    def setup(self, query):
        # setup = partial(self.build_request, 'GET', '{}/{}?hl=en&gl=US&ned=us'.format(query, query))
        return self.build_request('GET', '{}/{}?hl=en&gl=US&ned=us'.format(query, query))

    async def handle_response(self, resp, topic, queue, **kwargs):
        if resp.status != 200:
            raise ConnectionError('Could not connect to google: {}'.format(resp.status))
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


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=2, wait_jitter_max=500)
async def fetch(url, session):
    """Tries to download html for a single url with provided session"""
    try:
        async with session.get(url) as raw:
            enc_search = re.search('charset=(?P<enc>\S*)', raw.headers.get('content-type', default=''))
            encoding = enc_search.group('enc') if enc_search else 'UTF-8'
            res = await raw.text(encoding=encoding, errors='ignore')
            log.debug('Successfully downloaded html from %s', url)
            return res
    except (aiohttp.ClientError, aiohttp.ServerDisconnectedError):
        log.debug('Failed to download html from %s', url)
        return None


async def download(url_queue, result_queue, duration=0):
    """Continuously downloads urls placed in url queue and places resulting htmls into result queue"""
    jobs = []
    start = time.time()
    sess = aiohttp.ClientSession()
    while not duration or time.time() - start < duration or not url_queue.empty():
        if len(jobs) >= 100 or url_queue.empty():
            log.debug('Downloading %d articles', len(jobs))
            responses = await asyncio.gather(*jobs)
            jobs = []
            for resp in responses:
                if resp is not None:
                    new_item = Item(resp, item.topic, item.source)
                    await result_queue.put(new_item)
        item = await url_queue.get()
        log.debug('Fetching %s', item.content)
        jobs.append(fetch(item.content, sess))
    await sess.close()


class Twingly(BaseScraper):
    base_url = "https://api.twingly.com/"
    rate_limit = 60

    def __init__(self, auth):
        self.client = auth
        self.parser = Parser()

    def setup(self, query):
        query_string = "{} lang:en tspan:12h page-size:10000".format(query)
        # setup = partial(self.build_request, 'GET', 'blog/search/api/v3/search',
        #                params={'q': query_string, 'apiKey': self.client})
        return self.build_request('GET', 'blog/search/api/v3/search',
                                  params={'q': query_string, 'apiKey': self.client})

    async def handle_response(self, resp, topic, queue, **kwargs):
        if resp.status == 401:
            raise ConnectionError('Could not connect to twingly: {}'.format(resp.status))
        else:
            res = await resp.text()
        result = self.parser.parse(res)
        urls = [post.url for post in result.posts]
        for url in self.clean_urls(urls):
            if self.filter(url):
                item = Item(url, topic, 'blog')
                await queue.put(item)
