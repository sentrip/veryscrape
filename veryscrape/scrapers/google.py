from lxml import html

from ..session import Session
from ..scrape import HTMLScraper, ItemGenerator


class ArticleGen(ItemGenerator):
    def process_text(self, text):
        return text[0]

    def process_time(self, text):
        return text[1]


class Google(HTMLScraper):
    source = 'article'
    item_gen = ArticleGen

    def __init__(self, *, proxy_pool=None):
        super(Google, self).__init__(Session, proxy_pool=proxy_pool)

    def query_string(self, query):
        return 'https://news.google.com/news/search/section/q/' \
               '{}/{}?hl=en&gl=US&ned=us'.format(query, query)

    def extract_urls(self, text):
        urls = set()
        result = html.fromstring(text)
        for e in result.xpath('//*[@href]'):
            if e.get('href') is not None:
                urls.add(e.get('href'))
        return urls, [None] * len(urls)
