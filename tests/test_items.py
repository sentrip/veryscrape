import asyncio
import pytest
from veryscrape.items import ItemGenerator


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

