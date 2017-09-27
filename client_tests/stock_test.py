import random
import time
from multiprocessing.connection import Listener
from threading import Thread

import gym

from src.base import Producer


def distribute(n):
    topics = Producer.load_query_dictionary('query_topics.txt')
    types = ['reddit', 'twitter', 'blog', 'article', 'stock']
    server = Listener(('localhost', 6200), authkey=b'veryscrape')
    connections = []
    for _ in range(n):
        connections.append(server.accept())

    while True:
        d = {}
        for q in topics:
            d[q] = {}
            for t in types:
                d[q][t] = random.random() * (1 if t != 'stock' else 500)
        for c in connections:
            c.send(d)
        time.sleep(10)


def play():
    env = gym.make('StockGym-v0')
    env.reset()
    while True:
        _ = env.step(1)


if __name__ == '__main__':
    n = 5
    Thread(target=distribute, args=(n, )).start()
    #time.sleep(1)
    for _ in range(n):
        Thread(target=play).start()
        #time.sleep(0.1)
