import asyncio
import logging
import re
from datetime import datetime
from hashlib import md5

log = logging.getLogger(__name__)


class Item:
    def __init__(self, content='', topic='', source='', created_at=None):
        self.content = content
        self.topic = topic
        self.source = source
        self.created_at = datetime.now() if created_at is None else created_at

    def __str__(self):
        return "Item({:5s}, {:7s}, {:50s})".format(
            self.topic, self.source, re.sub(r'[\n\r\t]', '', str(self.content))
        )


class ItemGenerator:
    max_seen_items = 50000

    def __init__(self, q, topic='', source=''):
        self.q = q
        self.topic = topic
        self.source = source
        self.seen = set()
        self.cancelled = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        text = None
        created_at = None
        while text is None:
            if self.cancelled:
                raise StopAsyncIteration
            try:
                unclean_text = self.q.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(1e-2)
                continue
            text = self.process_text(unclean_text)
            created_at = self.process_time(unclean_text)
            if not self.filter(text):
                text = None
        return Item(content=text, topic=self.topic,
                    source=self.source, created_at=created_at)

    def process_text(self, text):
        return text

    def process_time(self, text):
        return

    def filter(self, text):
        if text is None:
            return False
        hsh = md5(text.encode()).hexdigest()
        if hsh not in self.seen:
            self.seen.add(hsh)
            if len(self.seen) >= self.max_seen_items:
                self.seen.pop()
            return True
        log.debug('Filtering already seen item: %s',
                  text[:50].replace('\n', ''))
        return False

    def cancel(self):
        self.cancelled = True


class ItemMerger:
    def __init__(self, *item_gens):
        self.q = asyncio.Queue()
        self.item_gens = item_gens
        self.cancelled = False
        self.merge_future = None

    async def stream(self, item_gen):
        async for item in item_gen:
            await self.q.put(item)

    def __aiter__(self):
        self.merge_future = asyncio.ensure_future(asyncio.gather(*[
            self.stream(item_gen) for item_gen in self.item_gens
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
        log.debug('Cancelling item merger: SOURCES=%s - TOPICS=%s',
                  str(set(i.source for i in self.item_gens)),
                  str(set(i.topic for i in self.item_gens))
                  )
        self.cancelled = True
        for gen in self.item_gens:
            gen.cancel()
        self.merge_future.cancel()


# todo: add TimeOrderedItems
