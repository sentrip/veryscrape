from urllib.parse import quote
from twingly_search import Parser

from ..session import Session
from ..scrape import SearchEngineScraper, ItemGenerator


class BlogGen(ItemGenerator):
    def process_text(self, text):
        return text[0]

    def process_time(self, text):
        return text[1]


class Twingly(SearchEngineScraper):
    source = 'blog'
    item_gen = BlogGen

    def __init__(self, api_key, *, proxy_pool=None):
        super(Twingly, self).__init__(Session, proxy_pool=proxy_pool)
        self.api_key = api_key
        self.parser = Parser()

    def query_string(self, query):
        qs = quote('{} lang:en tspan:12h page-size:1'.format(query))
        return 'https://api.twingly.com/blog/search/api/v3/search?' \
               'q=%s&apiKey=%s' % (qs, self.api_key)

    def extract_urls(self, text):
        result = self.parser.parse(text)
        return tuple(zip(*list((post.url, post.published_at)
                               for post in result.posts)))
