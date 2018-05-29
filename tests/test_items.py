import asyncio
import pytest
from veryscrape.items import ItemGenerator, ItemMerger, TimeOrderedItems


@pytest.mark.asyncio
async def test_item_generator():
    q = asyncio.Queue()
    for i in range(10):
        q.put_nowait(str(i))

    count = 0
    items = ItemGenerator(q, topic='test', source='t')
    async for item in items:
        assert item.content == str(count), "Item has incorrect content"
        assert item.topic == 'test', "Item has incorrect topic"
        assert item.source == 't', "Item has incorrect source"
        count += 1
        if count >= 9:
            break


@pytest.mark.asyncio
async def test_item_generator_filter():
    q = asyncio.Queue()
    q.put_nowait(None)
    for i in range(10):
        for _ in range(2):
            q.put_nowait(str(i))

    count = 0
    items = ItemGenerator(q, topic='test', source='t')
    items.max_seen_items = 9
    async for item in items:
        assert item.content == str(count), "Item has incorrect content"
        assert item.topic == 'test', "Item has incorrect topic"
        assert item.source == 't', "Item has incorrect source"
        count += 1
        if count >= 9:
            break


@pytest.mark.asyncio
async def test_item_generator_cancelled():
    q = asyncio.Queue()
    for i in range(2):
        q.put_nowait(str(i))

    items = ItemGenerator(q, topic='test', source='t').__aiter__()
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
async def test_time_ordered_items_amount(random_item_queue):
    ordered = TimeOrderedItems(random_item_queue, max_items=50)
    last, count = 0, 0
    async for item in ordered:
        current = item.created_at.timestamp()
        assert current > last, 'Did not return items ordered by time'
        last = current
        count += 1
        if count >= 50:
            ordered.cancel()


@pytest.mark.asyncio
async def test_time_ordered_items_age(random_item_queue):
    ordered = TimeOrderedItems(random_item_queue, max_age=50)
    last, count = 0, 0
    async for item in ordered:
        current = item.created_at.timestamp()
        assert current > last, 'Did not return items ordered by time'
        last = current
        count += 1
        if count >= 50:
            ordered.cancel()

