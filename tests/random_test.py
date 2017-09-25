import time
from threading import Thread, Lock
from multiprocessing import Queue

# stages: f - Finance, t - Stream, p - PreProcess, s - Sentiment, w - Write
stages = 'tps'
send_every = 10
port = 6000


if __name__ == '__main__':
    if 't' in stages:
        from src.services.stream import StreamWorker
        s = StreamWorker(use_processes=True)
        s.start()
        time.sleep(1)
    if 'p' in stages:
        from src.services.preprocess import PreProcessWorker
        p = PreProcessWorker()
        p.start()
        port += 1
        time.sleep(1)
    if 's' in stages:
        from src.services.sentimentprocess import SentimentAverage, SentimentWorker
        sent_queue = Queue()
        st = SentimentWorker(sent_queue)
        st.start()
        sa = SentimentAverage(sent_queue, send_every=send_every)
        sa.start()
        port += 1
        time.sleep(1)
    if 'f' in stages:
        from src.services.finance import FinanceWorker
        f = FinanceWorker(send_every=send_every)
        f.start()
        time.sleep(1)
        port = 6009
    if 'w' in stages:
        from src.services.write import WriteWorker
        w = WriteWorker(Lock(), send_every=send_every)
        w.start()

    def print_stuff():
        from multiprocessing.connection import Client
        c = Client(('localhost', port), authkey=b'veryscrape')
        while True:
            i = c.recv()
            print(i)
    if 'w' not in stages:
        Thread(target=print_stuff).start()

"""Reddit ClientConnectorError(110, "Cannot connect to host oauth.reddit.com:443 ssl:True [Can not connect to oauth.reddit.com:443 [Connect call failed ('151.101.61.140', 443)]]"
"""