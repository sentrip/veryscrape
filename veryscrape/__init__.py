import asyncio
import os
import re
from collections import namedtuple

BASE_DIR = "/home/djordje/Sentrip/"
if not os.path.isdir(BASE_DIR):
    BASE_DIR = "C:/users/djordje/desktop"
Item = namedtuple('Item', ['content', 'topic', 'source'])
Item.__repr__ = lambda s: "Item({:5s}, {:7s}, {:15s})".format(s.topic, s.source, re.sub(r'[\n\r\t]', '', str(s.content)[:15]))


class ExponentialBackOff:
    def __init__(self, ratio=2):
        self.ratio = ratio
        self.count = 0
        self.retry_time = 1

    def reset(self):
        self.count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.count:
            await asyncio.sleep(self.retry_time)
            self.retry_time *= self.ratio
        self.count += 1
        return self.count
