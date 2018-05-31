from functools import partial
import pytest
from veryscrape.scrapers import *

SCRAPERS = [
    partial(Twitter, '', '', '', ''),
    partial(Reddit, '', ''),
    partial(Twingly, ''),
    Google,
    partial(Spider, source_urls=['spider%d' % k for k in range(4)])
]


@pytest.mark.parametrize('scraper', SCRAPERS)
@pytest.mark.asyncio
async def test_scrape(patched_aiohttp, scraper):
    scraper = scraper()
    topic = '__classify__' if isinstance(scraper, Spider) else 'topic'
    await scraper.scrape('data', topic=topic)
    await scraper.client.close()
    assert scraper.queues[topic].qsize() > 0, "Scraper did not get any data"


@pytest.mark.parametrize('scraper', SCRAPERS)
@pytest.mark.asyncio
async def test_stream(patched_aiohttp, scraper):
    scraper = scraper()
    got_item = False
    topic = '__classify__' if isinstance(scraper, Spider) else 'topic'
    async for item in scraper.stream('data', topic=topic):
        got_item = True
        assert 'some data' in item.content, 'Did not parse correct data'
        break
    await scraper.close()
    assert got_item, 'Did not get any items from stream'
