import time
from multiprocessing.connection import Client

from base import Item
from preprocess import PreProcessWorker
from sentimentprocess import SentimentWorker
from stream import StreamWorker

#from write import WriteWorker

if __name__ == '__main__':
    # file_lock = Lock()
    # f = FinanceWorker(send_every=10)
    # f.start()
    #time.sleep(1)
    s = StreamWorker(use_processes=True)
    s.start()
    time.sleep(1)
    p = PreProcessWorker()
    p.start()
    time.sleep(1)
    st = SentimentWorker(send_every=10)
    st.start()
    time.sleep(1)
    #
    # w = WriteWorker(file_lock, send_every=10)
    # w.start()
    # time.sleep(1)
    c = Client(('localhost', 6002), authkey=b'veryscrape')
    t = time.time()
    last = Item()
    while True:
        i = c.recv()
        if i.topic != last.topic or i.source != last.source:
            print(i, '{:.2f}'.format(time.time()-t))
            t = time.time()
            last = i
