import asyncio
import os
import re
from collections import namedtuple
from multiprocessing import Process, Queue
from multiprocessing.connection import Listener
from random import shuffle
from threading import Thread

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


class AsyncStream:

    def __aiter__(self):
        return self

    def __anext__(self):
        return

    async def stream(self):
        while True:
            try:
                await self.__anext__()
            except StopAsyncIteration:
                break


class Producer(Process):
    def __init__(self, port, use_processes=True):
        super(Producer, self).__init__()
        self.result_queue = Queue()
        self.outgoing = None
        self.port = port
        self.W = Process if use_processes else Thread
        self.running = True

    def initialize_work(self):
        return []

    @staticmethod
    def load_query_dictionary(file_name):
        """Loads query topics and corresponding queries from disk"""
        queries = {}
        with open(os.path.join(BASE_DIR, 'lib', 'documents', file_name), 'r') as f:
            lns = f.read().splitlines()
            for l in lns:
                x, y = l.split(':')
                queries[x] = y.split(',')
        return queries

    @staticmethod
    def load_authentications(file_name, query_dictionary):
        """Load api keys seperated by '|' from file"""
        api_keys = {}
        with open(os.path.join(BASE_DIR, 'lib', 'api', file_name), 'r') as f:
            data = f.read().splitlines()
            for i, topic in enumerate(query_dictionary):
                api_keys[topic] = data[i].split('|')
        return api_keys

    @staticmethod
    def run_in_loop(jobs):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*jobs))

    def run(self):
        listener = Listener(('localhost', self.port), authkey=b'vs')
        self.outgoing = listener.accept()
        jobs = self.initialize_work()
        for set_of_jobs in jobs:
            if set_of_jobs:
                shuffle(set_of_jobs)
                self.W(target=self.run_in_loop, args=(set_of_jobs,)).start()
        while self.running:
            self.outgoing.send(self.result_queue.get())
