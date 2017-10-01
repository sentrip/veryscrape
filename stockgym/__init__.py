from multiprocessing.connection import Listener
from threading import Thread

import numpy as np

from veryscrape import load_query_dictionary


class Distributor(Thread):
    def __init__(self, queue, n_nodes=50):
        super(Distributor, self).__init__()
        self.n = n_nodes
        self.connections = []
        self.queue = queue
        self.server = Listener(('localhost', 6200), authkey=b'veryscrape')
        self.topics = sorted(list(load_query_dictionary('query_topics').keys()))
        self.types = ['article', 'blog', 'reddit', 'twitter', 'stock']
        self.running = True

    def accept_incoming(self, n=50):
        for _ in range(n):
            self.connections.append(self.server.accept())

    def normalize(self, data):
        new_data = np.zeros([110, 5])
        default = {k: {t: 0.01 for t in self.types} for k in self.topics}.update(data)
        for i, k in enumerate(self.topics):
            for j, t in enumerate(self.types):
                try:
                    new_data[i][j] = default[k][t]
                except (KeyError, TypeError):
                    new_data[i][j] = 0.001
        return new_data

    def run(self):
        self.accept_incoming(self.n)
        while self.running:
            if not self.queue.empty():
                data = self.queue.get_nowait()
                new_data = self.normalize(data)
                for c in self.connections:
                    c.send(new_data)

