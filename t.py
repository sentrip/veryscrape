import time
from multiprocessing.connection import Client

from preprocess import PreProcessWorker
from sentimentprocess import SentimentWorker
from stream import StreamWorker

if __name__ == '__main__':
    s = StreamWorker()
    s.start()
    time.sleep(1)
    p = PreProcessWorker()
    p.start()
    time.sleep(1)
    st = SentimentWorker(send_every=10)
    st.start()
    time.sleep(1)
    c = Client(('localhost', 6002), authkey=b'veryscrape')
    t = time.time()
    while True:
        i = c.recv()
        print(i, '{:.2f}'.format(time.time()-t))
        t = time.time()
