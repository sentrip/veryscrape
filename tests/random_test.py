import time
from threading import Thread, Lock

# stages: f - Finance, t - Stream, p - PreProcess, s - Sentiment, w - Write
stages = 't'
send_every = 10
port = 6000


if __name__ == '__main__':
    if 't' in stages:
        from src.services.stream import StreamWorker
        s = StreamWorker(use_processes=False)
        s.start()
        time.sleep(1)
    if 'p' in stages:
        from src.services.preprocess import PreProcessWorker
        p = PreProcessWorker()
        p.start()
        port += 1
        time.sleep(1)
    if 's' in stages:
        from src.services.sentimentprocess import SentimentWorker
        st = SentimentWorker(send_every=send_every)
        st.start()
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
