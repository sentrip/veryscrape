import asyncio
import json
import logging
import re
import time

import aiohttp
import lxml.html as html
from retrying import retry
from twingly_search.parser import Parser

from request import BaseScraper, Item

log = logging.getLogger('veryscrape')


class Twitter(BaseScraper):
    def __init__(self, auth, **kwargs):
        super(Twitter, self).__init__(**kwargs)
        self.client, self.secret, self.token, self.token_secret = auth
        self.base_url = 'https://stream.twitter.com/1.1/'
        self.proxy_params = {'speed': 30, 'https': 1, 'post': 1, 'anonymous': 1}
        # Rate limits
        self.retry_420 = 60
        self.snooze_time = 0.25

    def setup(self, query):
        params = {'language': 'en', 'track': query}
        return self.build_request('POST', 'statuses/filter.json', oauth=1, params=params)

    async def handle_response(self, resp, topic, break_after=0):
        print(resp.status)
        if resp.status == 420:
            await asyncio.sleep(self.retry_420)
        elif resp.status != 200:
            raise ConnectionError('Could not connect to twitter: {}'.format(resp.status))
        else:
            count = 0
            async for line in resp.content:
                print(line)
                if count >= break_after > 0:
                    break
                try:
                    status = json.loads(line.decode('utf-8'))
                    if self.filter(status['text']):
                        await self.queue.put(Item(status['text'], topic, 'twitter'))
                    count += 1
                except json.JSONDecodeError:
                    pass
            await asyncio.sleep(self.snooze_time)


class Reddit(BaseScraper):
    def __init__(self, auth, **kwargs):
        super(Reddit, self).__init__(**kwargs)
        self.client, self.secret = auth
        self.link = '{}/hot.json?raw_json=1&limit=100'
        self.comment_base = '%s/comments/{}.json?raw_json=1&limit=10000&depth=10'
        self.comment = ''

        self.base_url = 'https://oauth.reddit.com/r/'
        self.token_url = 'https://www.reddit.com/api/v1/access_token'
        self.token_expiry = time.time() - 5
        self.rate_limit = 60
        self.user_agent = 'test app'
        self.persist_user_agent = True

    def setup(self, query):
        self.comment = self.comment_base % query
        # setup = partial(self.build_request, 'GET', self.link.format(query), oauth=2)
        return self.build_request('GET', self.link.format(query), oauth=2)

    def setup_comments(self, query):
        return self.build_request('GET', self.comment.format(query), oauth=2)

    async def handle_comments(self, resp, topic):
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
                await self.queue.put(item)

    async def handle_response(self, resp, topic, break_after=0):
        resp = await resp.json()
        count = 0
        for i in resp['data']['children']:
            link = i['data']['id']
            try:
                if count >= break_after > 0:
                    break
                await self.scrape(link, topic, setup=self.setup_comments, resp_handler=self.handle_comments)
                count += 1
            except aiohttp.ClientError:
                await asyncio.sleep(0)


class Google(BaseScraper):
    def __init__(self, **kwargs):
        super(Google, self).__init__(**kwargs)
        self.base_url = 'https://news.google.com/news/search/section/q/'
        self.proxy_params = {'speed': 50, 'https': 1, 'anonymity': 2}
        self.rate_limit = 120

    def setup(self, query):
        return self.build_request('GET', '{}/{}?hl=en&gl=US&ned=us'.format(query, query))

    async def handle_response(self, resp, topic, **kwargs):
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
                await self.queue.put(item)


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

    def __init__(self, auth, **kwargs):
        super(Twingly, self).__init__(**kwargs)
        self.client = auth
        self.parser = Parser()

    def setup(self, query):
        query_string = "{} lang:en tspan:12h page-size:10000".format(query)
        return self.build_request('GET', 'blog/search/api/v3/search',
                                  params={'q': query_string, 'apiKey': self.client})

    async def handle_response(self, resp, topic, **kwargs):
        if resp.status == 401:
            raise ConnectionError('Could not connect to twingly: {}'.format(resp.status))
        else:
            res = await resp.text()
        result = self.parser.parse(res)
        urls = [post.url for post in result.posts]
        for url in self.clean_urls(urls):
            if self.filter(url):
                item = Item(url, topic, 'blog')
                await self.queue.put(item)
