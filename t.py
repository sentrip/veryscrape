import time
from threading import Lock

from extensions.proxy import ProxySnatcher
from finance import FinanceWorker
from preprocess import PreProcessWorker
from sentimentprocess import SentimentWorker
from stream import StreamWorker
from write import WriteWorker

if __name__ == '__main__':
    file_lock = Lock()
    proxy_thread = ProxySnatcher(110,
                                 **{'minDownloadSpeed': '50',
                                    'protocol': 'http',
                                    'allowsHttps': 1,
                                    'allowsUserAgentHeader': 1})
    proxy_thread.start()
    proxy_thread.wait_for_proxies()

    f = FinanceWorker(proxy_thread, send_every=10)
    f.start()
    time.sleep(1)

    s = StreamWorker(proxy_thread, use_processes=True)
    s.start()
    time.sleep(1)

    p = PreProcessWorker()
    p.start()
    time.sleep(1)
    st = SentimentWorker(send_every=10)
    st.start()
    time.sleep(1)

    w = WriteWorker(file_lock, send_every=10)
    w.start()
    time.sleep(1)
    # c = Client(('localhost', 6000), authkey=b'veryscrape')
    # t = time.time()
    # last = Item()
    # while True:
    #     i = c.recv()
    #     print(i)
        # if i.topic != last.topic or i.source != last.source:
        #     print(i, '{:.2f}'.format(time.time()-t))
        #     t = time.time()
        #     last = i
