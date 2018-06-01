from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
from functools import partial
import asyncio
import heapq
import logging
import time

from .process import classify_text, clean_item

log = logging.getLogger(__name__)


# the only purpose of this is to allow a defaultdict
# created with this function as the default factory
# to be passed into a ProcessPoolExecutor
def _create_list_defaultdict():
    return defaultdict(list)  # pragma: nocover


class GeneratorWrapper:
    def __init__(self, item_gen, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.cancelled = False
        self.items = item_gen
        self._gen = None
        self._q = asyncio.Queue()
        self._future = None

    def cancel(self):
        if not self.cancelled:
            self.cancelled = True
            self.items.cancel()
            if self._future is not None and not self._future.done():
                self._future.cancel()

    async def get(self):
        try:
            return self._q.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1e-3)

    async def put(self, item):
        await self._q.put(item)

    def __aiter__(self):
        self._gen = self.items.__aiter__()
        self._future = asyncio.ensure_future(self._stream())
        return self

    async def __anext__(self):
        while True:
            if self.cancelled:
                raise StopAsyncIteration
            else:
                item = await self.get()
                if item is not None:
                    return item

    async def _stream(self):
        while True:
            try:
                item = await self._gen.__anext__()
                await self.put(item)
            except StopAsyncIteration:
                return


class ItemMerger:
    def __init__(self, *item_gens):
        self.q = asyncio.Queue()
        self.item_gens = item_gens
        self.cancelled = False
        self._future = None

    def __aiter__(self):
        self._future = asyncio.ensure_future(asyncio.gather(*[
            self._stream(item_gen) for item_gen in self.item_gens
        ]))
        return self

    async def __anext__(self):
        while not self.cancelled:
            try:
                return self.q.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(1e-3)
        raise StopAsyncIteration

    def cancel(self):
        if not self.cancelled:
            self.cancelled = True
            for gen in self.item_gens:
                gen.cancel()
            self._future.cancel()

    async def _stream(self, item_gen):
        async for item in item_gen:
            await self.q.put(item)


class ItemProcessor(GeneratorWrapper):
    # The default classification function is simple and fast
    # You can change this if you want more detailed classification
    # classify takes two arguments - data: any, topics_to_classify: dict
    # see veryscrape.process.classify_text for more details
    classify = classify_text

    def __init__(self, items, n_cores=1, loop=None):
        super(ItemProcessor, self).__init__(items, loop=loop)
        self.pool = ProcessPoolExecutor(max_workers=n_cores)
        self.loop.set_default_executor(self.pool)
        self.topics_by_source = defaultdict(_create_list_defaultdict)

    def cancel(self):
        self.pool.shutdown(wait=True)
        super(ItemProcessor, self).cancel()

    async def put(self, item):
        f = self.loop.run_in_executor(self.pool, clean_item, item)
        if item.topic == '__classify__':
            f.add_done_callback(self._classify_item)
        else:
            f.add_done_callback(self._enqueue_item)
        await asyncio.sleep(0)

    def update_topics(self, **topics_by_source):
        """
        Update local topics by source for use in classification of items
        :param topics_by_source: dict[list]: associated queries by topic
        """
        self.topics_by_source.update(topics_by_source)

    def _classify_item(self, future):
        if self._should_continue(future):
            item = future.result()
            f = self.loop.run_in_executor(
                self.pool, ItemProcessor.classify,
                item.content, self.topics_by_source[item.source]
            )
            f.add_done_callback(partial(
                self._enqueue_classified_item, item=item))

    def _enqueue_classified_item(self, future, item=None):
        if self._should_continue(future) and item is not None:
            item.topic = future.result()
            log.debug('Queuing cleaned and classified item: %s', str(item))
            self._q.put_nowait(item)

    def _enqueue_item(self, future):
        if self._should_continue(future):
            result = future.result()
            log.debug('Queuing cleaned item: %s', str(result))
            self._q.put_nowait(result)

    def _should_continue(self, future):
        return (
            not self.cancelled
            and not future.cancelled()
            and not future.exception()
        )


class ItemSorter(GeneratorWrapper):
    def __init__(self, items, max_items=None, max_age=None, loop=None):
        super(ItemSorter, self).__init__(items, loop=loop)
        self.max_items = max_items or 0
        self.max_age = max_age or 0
        self._heap = []

    async def put(self, item):
        heapq.heappush(self._heap, (item.created_at.timestamp(), item))
        await asyncio.sleep(0)

    async def get(self):
        if self._heap and (
            len(self._heap) > self.max_items
            or time.time() - self._heap[0][0] > self.max_age
        ):
            return heapq.heappop(self._heap)[1]

        await asyncio.sleep(1e-3)
