import asyncio
import time
from multiprocessing import Queue
from threading import Thread

from base import Producer
from extensions.twitter2 import QueryStream


def print_queue(q):
    while True:
        try:
            print(q.get_nowait())
        except:
            time.sleep(1)


if __name__ == '__main__':
    i = 'FB'
    qd = Producer.load_query_dictionary('query_topics.txt')
    a = Producer.load_authentications('twitter.txt', qd)
    queue = Queue()
    loop = asyncio.get_event_loop()
    Thread(target=print_queue, args=(queue,)).start()
    loop.run_until_complete(QueryStream(a[i], i, qd[i][0], queue).stream())
