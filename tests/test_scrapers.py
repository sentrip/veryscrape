import pytest

from veryscrape.scrapers import *


@pytest.mark.parametrize(
    'scraper,auth', [
        (Twitter, ('', '', '', '')),
        (Reddit, ('', '')),
        (Twingly, ('',)),
        (Google, ())
    ]
)
@pytest.mark.asyncio
async def test_scrape(patched_aiohttp, scraper, auth):
    scraper = scraper(*auth)
    await scraper.scrape('query', topic='topic')
    await scraper.client.close()
    assert scraper.queues['topic'].qsize() > 0, "Scraper did not get any data"


@pytest.mark.parametrize(
    'scraper,auth', [
        (Twitter, ('', '', '', '')),
        (Reddit, ('', '')),
        (Twingly, ('',)),
        (Google, ())
    ]
)
@pytest.mark.asyncio
async def test_stream(patched_aiohttp, scraper, auth):
    scraper = scraper(*auth)
    got_item = False
    async for item in scraper.stream('query', topic='topic'):
        got_item = True
        assert 'some data' in item.content, 'Did not parse correct data'
        break
    await scraper.close()
    assert got_item, 'Did not get any items from stream'
