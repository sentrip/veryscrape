from multiprocessing import Queue
from multiprocessing.connection import Listener, Client
from threading import Thread

import numpy as np

from vs import load_query_dictionary


class Distributor(Thread):
    def __init__(self, queue, n_nodes=50):
        super(Distributor, self).__init__()
        self.n = n_nodes
        self.queue = queue
        self.server = Listener(('localhost', 6200), authkey=b'veryscrape')
        self.topics = sorted(list(load_query_dictionary('query_topics').keys()))
        self.types = ['article', 'blog', 'reddit', 'twitter', 'stock']

    def create_matrix(self, data):
        m = np.zeros([len(self.topics), len(self.types)])
        for i, t in enumerate(self.topics):
            for j, q in enumerate(self.types):
                m[i][j] = data[q][t]
        return m

    def run(self):
        conns = []
        for _ in range(self.n):
            conns.append(self.server.accept())

        while True:
            data = self.queue.get()
            mat = self.create_matrix(data)
            for c in conns:
                c.send(mat)


if __name__ == '__main__':
    q = Queue()
    p = Distributor(q, 4)
    mock = {k: {t: 0.1 for t in p.topics} for k in p.types}
    p.start()
    cs = []
    for _ in range(4):
        cs.append(Client(('localhost', 6200), authkey=b'veryscrape'))

    while True:
        input()
        q.put(mock)
        for c in cs:
            print(c.recv().shape)
