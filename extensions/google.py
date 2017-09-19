# Class to stream text data from Google's services (news, blog)
import asyncio
from collections import deque

import aiohttp
from fake_useragent import UserAgent
from lxml import html

from base import Item


class GoogleClient:
    """Custom Google News Search client"""
    API_URL = 'https://news.google.com/news/search/section/q/{}/{}?hl=en&ned=us'

    def __init__(self, user_agent, proxy):
        self.user_agent = user_agent
        self.session = aiohttp.ClientSession(headers={'User-Agent': self.user_agent})
        self.proxy = proxy

    async def execute_query(self, q):
        """Executes the given search query and returns the urls"""
        domains = {'.com/', '.org/', '.edu/', '.gov/', '.net/', '.biz/'}
        false_urls = {'google.', 'blogger.', 'youtube.', 'googlenewsblog.'}
        query_url = self.API_URL.format(q, q)
        async with self.session.get(query_url) as response:  # proxy=self.proxy
            raw = await response.text()
        scraped_urls = set()
        try:
            html_tree = html.fromstring(raw)
            for e in html_tree.xpath('//*[@href]'):
                i = e.get('href') if e.get('href') is not None else ''
                is_root_url = any(i.endswith(j) for j in domains)
                is_not_relevant = any(j in i for j in false_urls)
                if i.startswith('http') and not (is_root_url or is_not_relevant):
                    scraped_urls.add(i)
        except Exception as e:
            pass
        return scraped_urls


async def google(parent, topic, query, search_every=15*60):
    fake_users = UserAgent()
    retry_time = 10.0
    client = GoogleClient(fake_users.random, None)
    seen_urls = deque(maxlen=100000)

    while parent.running:
        try:
            results = await client.execute_query(query)
            for url in results:
                if url not in seen_urls:
                    new_item = Item(content=url, topic=topic, source='article')
                    seen_urls.append(url)
                    parent.url_queue.put(new_item)
            await asyncio.sleep(search_every)

        except Exception as e:
            print('Google', repr(e))
            await asyncio.sleep(retry_time)
            client = GoogleClient(fake_users.random, None)
