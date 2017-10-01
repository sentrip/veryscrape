import time
from multiprocessing.connection import Listener
from threading import Thread

import numpy as np


class Sender:
    def __init__(self):
        self.conn = None
        self.server = Listener(('localhost', 6200), authkey=b'veryscrape')
        Thread(target=self.connn).start()

    def connn(self):
        while True:
            c = self.server.accept()
            self.conn = c

    def send(self):
        while self.conn is None:
            time.sleep(0.1)
        mat = np.random.rand(110, 5)
        mat[:, 4] *= 150
        self.conn.send(mat)


if __name__ == '__main__':
    s = Sender()
    while True:
        s.send()
        input()
