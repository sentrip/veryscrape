import asyncio
import pytest
import os
import json
from veryscrape.scrape import Scraper
from veryscrape import VeryScrape, register, unregister


@pytest.mark.asyncio
async def test_load_config(temp_config, scrape_config):
    vs = VeryScrape(asyncio.Queue(), config='scrape_config.json')
    assert vs.config == scrape_config, "Did not load correct config"
    # This should not raise errors
    vs._setup()


@pytest.mark.asyncio
async def test_load_config_failed():
    config = {'twitter': ['random', 'incorrect', 'noplease']}
    with open('scrape_config.json', 'w') as f:
        json.dump(config, f)
    vs = VeryScrape(asyncio.Queue(), config='scrape_config.json')
    assert vs.config == config, "Did not load correct config"
    with pytest.raises(ValueError):
        vs._setup()
    os.remove('scrape_config.json')


@pytest.mark.asyncio
async def test_scrape(patched_aiohttp, scrape_config):
    q = asyncio.Queue()
    vs = VeryScrape(q, config=scrape_config)

    async def wait_for_items():
        while q.qsize() < 4:
            await asyncio.sleep(1e-3)
        vs.close()

    await asyncio.gather(
        vs.scrape(), wait_for_items(), return_exceptions=True
    )

    while not q.empty():
        item = q.get_nowait()
        assert item.topic == 'topic1', 'Incorrect topic for item'
        assert 'some data' in item.content, 'Data did not pass through cleaning'


@pytest.mark.asyncio
async def test_scrape_and_classify(patched_aiohttp):
    q = asyncio.Queue()
    vs = VeryScrape(q, config={
        'spider': {'': {
            'kwargs': {
              'source_urls': ['spider%d' % k for k in range(10)]
            },

            'real': ['data'],
            'fake': ['kfksdfks'],
        }}})

    async def wait_for_items():
        while q.empty():
            await asyncio.sleep(1e-3)
        vs.close()

    await asyncio.gather(
        vs.scrape(), wait_for_items(), return_exceptions=True
    )

    item = q.get_nowait()
    assert item.topic == 'real', 'Incorrect topic for item'
    assert 'some data' in item.content, 'Data did not pass through cleaning'


@pytest.mark.asyncio
async def test_scrape_with_proxies(patched_aiohttp, patched_proxy_pool, scrape_config):
    q = asyncio.Queue()
    list(scrape_config['twitter'].values())[0].update(use_proxies=True)
    vs = VeryScrape(q, config=scrape_config)

    async def wait_for_items():
        while q.qsize() < 4:
            await asyncio.sleep(1e-3)
        vs.close()

    await asyncio.gather(
        vs.scrape(), wait_for_items(), return_exceptions=True
    )

    while not q.empty():
        item = q.get_nowait()
        assert item.topic == 'topic1', 'Incorrect topic for item'
        assert 'some data' in item.content, 'Data did not pass through cleaning'


@pytest.mark.asyncio
async def test_scrape_multi_core(patched_aiohttp, scrape_config):
    q = asyncio.Queue()
    vs = VeryScrape(q, config=scrape_config, n_cores=2)

    async def wait_for_items():
        while q.qsize() < 4:
            await asyncio.sleep(1e-3)
        vs.close()

    await asyncio.gather(
        vs.scrape(), wait_for_items(), return_exceptions=True
    )

    while not q.empty():
        item = q.get_nowait()
        assert item.topic == 'topic1', 'Incorrect topic for item'
        assert 'some data' in item.content, 'Data did not pass through cleaning'


@pytest.mark.asyncio
async def test_register():
    class TestScraper(Scraper):
        def __init__(self, *args, **kwargs):
            super(TestScraper, self).__init__(lambda *a, **kwa: {})

        async def scrape(self, query, topic='', **kwargs):
            for k in range(10):
                await self.queues[topic].put('stuff%d' % k)

    register('test', TestScraper)

    q = asyncio.Queue()
    vs = VeryScrape(q, config={'test': {'': {'topic': ['stuff']}}})
    vs._setup()
    assert len(vs.scrapers) == 1, 'Did not register new scraper'


@pytest.mark.asyncio
async def test_unregister():
    q = asyncio.Queue()

    unregister('twitter')

    vs = VeryScrape(q, config={'twitter': {'': {'topic': ['stuff']}}})
    with pytest.raises(ValueError):
        vs._setup()
    assert len(vs.scrapers) == 0, 'Registered nonexistent scraper'

    unregister('spider')

    vs = VeryScrape(q, config={'spider': {'': {'topic': ['stuff']}}})
    with pytest.raises(ValueError):
        vs._setup()
    assert len(vs.scrapers) == 0, 'Registered nonexistent scraper'

    unregister('*')

    vs = VeryScrape(q, config={'article': {'': {'topic': ['stuff']}}})
    with pytest.raises(ValueError):
        vs._setup()
    assert len(vs.scrapers) == 0, 'Registered nonexistent scraper'


@pytest.mark.skip
@pytest.mark.asyncio
async def test_scrape_interrupted(patched_aiohttp):
    pass


@pytest.mark.skip
@pytest.mark.asyncio
async def test_scrape_with_proxies_interrupted(patched_aiohttp):
    pass
