from datetime import datetime
import json

from ..items import ItemGenerator
from ..process import remove_urls
from ..scrape import Scraper
from ..session import OAuth1Session, fetch


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
        if self.last_item is not None:
            return datetime.fromtimestamp(
                int(self.last_item['timestamp_ms']) / 1000)


class Twitter(Scraper):
    source = 'twitter'
    item_gen = TweetGen
    scrape_every = 600

    def __init__(self, key, secret, token, token_secret, *, proxy_pool=None):
        super(Twitter, self).__init__(
            OAuth1Session,
            key, secret, token, token_secret,
            proxy_pool=proxy_pool
        )
        self.client.base_url = 'https://stream.twitter.com/1.1/'

    async def scrape(self, query, topic='', **kwargs):
        await fetch('POST', 'statuses/filter.json',
                    session=self.client,
                    stream_func=lambda l: self.queues[topic].put_nowait(l),
                    params={'language': 'en', 'track': query}, **kwargs
                    )
