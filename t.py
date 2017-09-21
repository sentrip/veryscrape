import time
from multiprocessing.connection import Client

# from preprocess import PreProcessWorker
# from sentimentprocess import SentimentWorker
from extensions.finance import FinanceWorker

if __name__ == '__main__':
    f = FinanceWorker(send_every=10)
    f.start()
    # s = StreamWorker(use_processes=False)
    # s.start()
    # time.sleep(1)
    # p = PreProcessWorker()
    # p.start()
    # time.sleep(1)
    # st = SentimentWorker(send_every=10)
    # st.start()
    # time.sleep(1)
    # l = deque()
    # for _ in range(2*len(s.topics)+1):
    #     l.append(Item())
    c = Client(('localhost', 6009), authkey=b'veryscrape')
    t = time.time()
    while True:
        i = c.recv()
        #l.append(i)
        print(i, '{:.2f}'.format(time.time()-t))
        #_ = l.popleft()
        t = time.time()
