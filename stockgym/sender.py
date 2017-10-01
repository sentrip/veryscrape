import time
from multiprocessing.connection import Listener
from threading import Thread

import numpy as np


class Sender:
    def __init__(self):
        self.conn = None
        self.server = Listener(('localhost', 6200), authkey=b'veryscrape')
        Thread(target=self.connn).start()
        self.add = 3
        self.count = 0

    def connn(self):
        while True:
            c = self.server.accept()
            self.conn = c
            self.add = 0.25

    def send(self):
        while self.conn is None:
            time.sleep(0.1)

        mat = np.array([[0.5, 0.5, 0.5, 0.5, 150. + self.add]]*110)

        if self.count > 10:
            self.add -= 3
        else:
            self.add += 3

        self.count += 1
        self.conn.send(mat)


if __name__ == '__main__':
    s = Sender()
    while True:
        s.send()
        input()

    # import gym
    # import empyrical
    # import numpy as np
    #
    # env = gym.make('StockGym-v0')
    # env.reset()
    # env.step(8)
    # for i in range(20):
    #     if i == 9:
    #         env.step(0)
    #     else:
    #         env.step(4)
    #     print(env.agent_value, env.market_value, env.agent_market_ratio)
    #     print(env.agent_return, env.benchmark_return, empyrical.cum_returns(np.array(env.returns)))
    #     print()