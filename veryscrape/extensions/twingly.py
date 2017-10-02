import asyncio
import time

from twingly_search.parser import Parser, TwinglySearchAuthenticationException

from veryscrape import Item, async_run_forever
from veryscrape.client import SearchClient


class Twingly(SearchClient):
    base_url = "https://api.twingly.com/"
    blog_search_path = 'blog/search/api/v3/search'

    def __init__(self, auth, queue):
        super(Twingly, self).__init__()
        self.client = auth
        self.parser = Parser()
        self.queue = queue

    def extract_urls(self, resp):
        try:
            result = self.parser.parse(resp)
        except TwinglySearchAuthenticationException:
            print('Twingly not authenticated!')
            time.sleep(1200)
            raise TwinglySearchAuthenticationException(401)
        urls = [post.url for post in result.posts]
        return self.clean_urls(urls)

    @staticmethod
    def build_query(query):
        return "{} lang:en tspan:12h page-size:10000".format(query)

    async def blog_stream(self, track=None, topic=None, duration=10800):
        start_time = time.time()
        while True:
            params = {'apiKey': self.client, 'q': self.build_query(track)}
            async with await self.get(self.blog_search_path, params=params) as raw:
                resp = await raw.text()
            urls = self.extract_urls(resp)
            for url in urls:
                await self.queue.put(Item(url, topic, 'blog'))

            if time.time() - start_time >= duration:
                break
            else:
                await asyncio.sleep(900)

    @async_run_forever
    async def stream(self, track=None, topic=None, duration=3600):
        await self.blog_stream(track, topic, duration)
