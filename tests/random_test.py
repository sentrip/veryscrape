if __name__ == '__main__':
    import time
    from threading import Thread, Lock
    from src.services.finance import FinanceWorker
    from src.services.stream import StreamWorker
    from src.services.sentimentprocess import SentimentWorker
    from src.services.preprocess import PreProcessWorker
    from src.services.write import WriteWorker
    # stages: f - Finance, t - Stream, p - PreProcess, s - Sentiment, w - Write
    stages = 'tps'
    send_every = 10
    port = 6000

    if 't' in stages:
        s = StreamWorker(use_processes=True)
        s.start()
        time.sleep(1)
    if 'p' in stages:
        p = PreProcessWorker()
        p.start()
        port += 1
        time.sleep(1)
    if 's' in stages:
        st = SentimentWorker(send_every=send_every)
        st.start()
        port += 1
        time.sleep(1)
    if 'f' in stages:
        f = FinanceWorker(send_every=send_every)
        f.start()
        time.sleep(1)
        port = 6009
    if 'w' in stages:
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
