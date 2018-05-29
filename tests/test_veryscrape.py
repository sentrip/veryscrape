import asyncio
import pytest
import os
import json
from veryscrape import VeryScrape


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
            await asyncio.sleep(1e-4)
        vs.close()

    await asyncio.gather(
        vs.scrape(), wait_for_items(), return_exceptions=True
    )

    while not q.empty():
        item = q.get_nowait()
        assert item.topic == 'topic1', 'Incorrect topic for item'
        assert 'some data' in item.content, 'Data did not pass through cleaning'


@pytest.mark.asyncio
async def test_scrape_with_proxies(patched_aiohttp, patched_proxy_pool, scrape_config):
    q = asyncio.Queue()
    list(scrape_config['twitter'].values())[0].update(use_proxies=True)
    vs = VeryScrape(q, config=scrape_config)

    async def wait_for_items():
        while q.qsize() < 4:
            await asyncio.sleep(1e-4)
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
            await asyncio.sleep(1e-4)
        vs.close()

    await asyncio.gather(
        vs.scrape(), wait_for_items(), return_exceptions=True
    )

    while not q.empty():
        item = q.get_nowait()
        assert item.topic == 'topic1', 'Incorrect topic for item'
        assert 'some data' in item.content, 'Data did not pass through cleaning'


@pytest.mark.asyncio
async def test_scrape_interrupted(patched_aiohttp):
    pass


@pytest.mark.asyncio
async def test_scrape_with_proxies_interrupted(patched_aiohttp):
    pass


@pytest.mark.asyncio
async def test_scrape_multi_core_interrupted(patched_aiohttp):
    pass
