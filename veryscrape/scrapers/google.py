from ..process import extract_urls
from ..scrape import SearchEngineScraper, ItemGenerator
from ..session import Session


class GoogleSession(Session):
    error_on_failure = False
    retries_to_error = 2


class ArticleGen(ItemGenerator):
    def process_text(self, text):
        return text[0]

    def process_time(self, text):
        return text[1]


class Google(SearchEngineScraper):
    source = 'article'
    item_gen = ArticleGen
    session_class = GoogleSession

    def query_string(self, query):
        return 'https://news.google.com/news/search/section/q/' \
               '{}/{}?hl=en&gl=US&ned=us'.format(query, query)

    def extract_urls(self, text):
        urls = extract_urls(text)
        return urls, [None] * len(urls)
