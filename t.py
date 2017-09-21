import time
from threading import Lock

# from preprocess import PreProcessWorker
# from sentimentprocess import SentimentWorker
from extensions.finance import FinanceWorker

#from write import WriteWorker

if __name__ == '__main__':
    file_lock = Lock()
    f = FinanceWorker(send_every=10)
    f.start()
    time.sleep(1)
    # s = StreamWorker(use_processes=True)
    # s.start()
    # time.sleep(1)
    # p = PreProcessWorker()
    # p.start()
    # time.sleep(1)
    # st = SentimentWorker(send_every=10)
    # st.start()
    # time.sleep(1)
    #
    # w = WriteWorker(file_lock, send_every=10)
    # w.start()
    # time.sleep(1)
    # c = Client(('localhost', 6002), authkey=b'veryscrape')
    # t = time.time()
    # while True:
    #     i = c.recv()
    #     print(i, '{:.2f}'.format(time.time()-t))
    #     t = time.time()
