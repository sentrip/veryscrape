import time
from multiprocessing.connection import Client

from stream import StreamWorker

if __name__ == '__main__':
    s = StreamWorker()
    s.start()
    time.sleep(1)
    c = Client(('localhost', 6000), authkey=b'veryscrape')
    while True:
        # try:
        #     print(s.url_queue.get_nowait())
        # except:
        #     pass

        print(c.recv())
