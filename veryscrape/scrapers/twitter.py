from datetime import datetime
import json

from ..items import ItemGenerator
from ..process import remove_urls
from ..scrape import Scraper
from ..session import OAuth1Session


class TwitterSession(OAuth1Session):
    base_url = 'https://stream.twitter.com/1.1/'


class TweetGen(ItemGenerator):
    last_item = None

    def process_text(self, text):
        try:
            self.last_item = json.loads(text.decode('utf-8'))
            text = self.last_item['text']
            return remove_urls(text)
        except (ValueError, KeyError):
            return

    def process_time(self, text):
        # Last json item is cached to avoid decoding more than once
        # process_time is always called immediately after process_text
        if self.last_item and self.last_item.get('timestamp_ms', None):
            return datetime.fromtimestamp(
                int(self.last_item['timestamp_ms']) / 1000)


class Twitter(Scraper):
    source = 'twitter'
    item_gen = TweetGen
    session_class = TwitterSession

    def __init__(self, key, secret, token, token_secret, *, proxy_pool=None):
        super(Twitter, self).__init__(
            key, secret, token, token_secret,
            proxy_pool=proxy_pool
        )

    async def scrape(self, query, topic='', **kwargs):
        await self.client.fetch(
            'POST', 'statuses/filter.json',
            stream_func=lambda l: self.queues[topic].put_nowait(l),
            params={'language': 'en', 'track': query}, timeout=None, **kwargs
        )
