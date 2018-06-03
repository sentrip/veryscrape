import asyncio
import pytest
from veryscrape.items import ItemGenerator
from veryscrape.wrappers import GeneratorWrapper, ItemMerger, ItemProcessor, ItemSorter


@pytest.mark.asyncio
async def test_generator_wrapper_cancelled(random_item_gen):
    items = GeneratorWrapper(random_item_gen).__aiter__()
    # This should work
    await items.__anext__()
    items.cancel()
    # Now it should fail even though there's an item
    with pytest.raises(StopAsyncIteration):
        await items.__anext__()


@pytest.mark.asyncio
async def test_item_merger():
    q1 = asyncio.Queue()
    q2 = asyncio.Queue()
    for i in range(10):
        q1.put_nowait(str(i * 100))
        q2.put_nowait('pre' + str(i))

    npcount, pcount = 0, 0
    no_pre = ItemGenerator(q1, topic='nopre', source='t')
    pre = ItemGenerator(q2, topic='pre', source='t')
    items = ItemMerger(no_pre, pre)

    async for item in items:
        if item.topic == 'nopre':
            assert item.content == str(npcount * 100)
            npcount += 1
        else:
            assert item.content == 'pre' + str(pcount)
            pcount += 1

        if pcount + npcount >= 19:
            break


@pytest.mark.asyncio
async def test_item_merger_cancelled():
    q1 = asyncio.Queue()
    q2 = asyncio.Queue()
    for i in range(2):
        q1.put_nowait(str(i * 100))
        q2.put_nowait('pre' + str(i))

    no_pre = ItemGenerator(q1, topic='nopre', source='t')
    pre = ItemGenerator(q2, topic='pre', source='t')
    items = ItemMerger(no_pre, pre).__aiter__()
    # This should work
    await items.__anext__()
    items.cancel()
    # Now it should fail even though there's an item
    with pytest.raises(StopAsyncIteration):
        await items.__anext__()


@pytest.mark.asyncio
async def test_item_processor(random_item_gen):
    import veryscrape.process
    veryscrape.process.register('test', lambda t: t + 0.5)
    random_item_gen.source = 'test'
    items = ItemProcessor(random_item_gen)
    count = 0
    async for item in items:
        assert item.content - int(item.content) == 0.5, 'Did not process item'
        count += 1
        if count >= 50:
            items.cancel()


@pytest.mark.asyncio
async def test_item_processor_classify(random_item_gen):
    import veryscrape.process

    def process(text):
        if int(text) % 2 == 0:
            return 'data two'
        else:
            return 'data three'

    veryscrape.process.register('test', process)
    random_item_gen.source = 'test'
    random_item_gen.topic = '__classify__'

    items = ItemProcessor(random_item_gen)
    items.update_topics(**{'test': {'even': ['two'], 'odd': ['three']}})
    count = 0
    async for k in items:
        assert k.content.startswith('data'), 'Did not process item'
        if k.content.endswith('two'):
            assert k.topic == 'even', 'Did not classify item'
        else:
            assert k.topic == 'odd', 'Did not classify item'
        assert k.topic
        count += 1
        if count >= 50:
            items.cancel()


@pytest.mark.asyncio
async def test_item_sorter_amount(random_item_gen):
    max_items = 50
    ordered = ItemSorter(random_item_gen, max_items=max_items)
    last, count = 0, 0
    async for item in ordered:
        current = item.created_at.timestamp()
        assert current > last, 'Did not return items ordered by time'
        last = current
        count += 1
        if count >= max_items / 2:
            ordered.cancel()


@pytest.mark.asyncio
async def test_item_sorter_age(random_item_gen):
    max_age = 50
    ordered = ItemSorter(random_item_gen, max_age=max_age)
    last, count = 0, 0
    async for item in ordered:
        current = item.created_at.timestamp()
        assert current > last, 'Did not return items ordered by time'
        last = current
        count += 1
        if count >= max_age / 2:
            ordered.cancel()
