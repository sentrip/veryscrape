from stream import StreamWorker
from multiprocessing.connection import Client

if __name__ == '__main__':
    s = StreamWorker()
    s.start()
    c = Client(('localhost', 6000), authkey=b'veryscrape')
    while True:
        print(s.url_queue.get())
