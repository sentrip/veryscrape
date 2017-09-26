if __name__ == '__main__':
    import time
    from threading import Lock
    from multiprocessing import Queue
    from src.services.finance import FinanceWorker
    from src.services.preprocess import PreProcessWorker
    from src.services.sentimentprocess import SentimentWorker, SentimentAverage
    from src.services.stream import StreamWorker
    from src.services.write import WriteWorker
    send_every = 60
    file_lock = Lock()
    sent_queue = Queue()

    f = FinanceWorker(send_every=send_every)
    f.start()
    time.sleep(1)

    s = StreamWorker(use_processes=True)
    s.start()
    time.sleep(1)

    p = PreProcessWorker()
    p.start()
    time.sleep(1)

    st = SentimentWorker(sent_queue)
    st.start()
    sa = SentimentAverage(sent_queue, send_every=send_every)
    sa.start()

    w = WriteWorker(file_lock, send_every=send_every)
    w.start()
