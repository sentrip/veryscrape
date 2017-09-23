from multiprocessing.connection import Client


if __name__ == '__main__':
    import time
    from preprocess import PreProcessWorker
    from sentimentprocess import SentimentWorker
    from stream import StreamWorker

    send_every = 60
    port = 6000

    s = StreamWorker(None, use_processes=True)
    s.start()
    time.sleep(1)

    p = PreProcessWorker()
    p.start()
    port += 1
    time.sleep(1)

    st = SentimentWorker(send_every=send_every)
    st.start()
    port += 1
    time.sleep(1)

    c = Client(('localhost', port), authkey=b'veryscrape')
    items = []
    old_items = []
    while True:
        if len(items) == 110:
            alive = sum((item.content != 0 and item.content != old.content) for item, old in zip(items, old_items))
            total = sum(item.content != 0 for item in items)
            print(alive, total)
            old_items = items
            items = []
        i = c.recv()
        if i.source == 'twitter':
            items.append(i)
