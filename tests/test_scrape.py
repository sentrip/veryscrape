import pytest
from veryscrape.scrape import SearchEngineScraper


@pytest.mark.asyncio
async def test_scrape(scraper):
    scraper = scraper()
    await scraper.scrape('query', topic='topic')
    for i in range(5):
        assert scraper.queues['topic'].get_nowait() == 'test%d' % i, \
            'Did not put correct data in queue'

    await scraper.client.close()


@pytest.mark.asyncio
async def test_stream(scraper):
    scraper = scraper()
    count = 0
    async for item in scraper.stream('query', topic='topic'):
        assert item.content == 'test%d' % count, 'Data not correctly wrapped in Item'
        if count >= 4:
            break
        count += 1
    await scraper.close()


@pytest.mark.asyncio
async def test_html_scrape(html_scraper):
    scraper = html_scraper()
    await scraper.scrape('urls', topic='topic')
    for i in range(2):
        item = scraper.queues['topic'].get_nowait()
        assert item == ('<html>stuff%d</html>' % (i + 2), i), \
            'Did not put correct data in queue'
    await scraper.client.close()


@pytest.mark.asyncio
async def test_html_stream(html_scraper):
    scraper = html_scraper()
    scraper.scrape_every = 1e-4
    count = 0
    async for item in scraper.stream('urls', topic='topic'):
        assert item.content == '<html>stuff%d</html>' % (count + 2), \
            'Data not correctly wrapped in Item'
        assert item.created_at == count, \
            'Time created not correctly wrapped'
        if count >= 1:
            break
        count += 1
    await scraper.close()


def test_clean_urls():
    urls = [
        'http://somewebsite.com/',
        'http://blogger.something.com/article.html',
        'http://theonlycorrectone.com/article.html'
    ]
    for url in SearchEngineScraper.clean_urls(urls):
        assert url == 'http://theonlycorrectone.com/article.html', \
            'Did not remove useless urls'

