from collections import defaultdict
from multiprocessing.connection import Client

if __name__ == '__main__':
    import time
    from src.services.finance import FinanceWorker2
    from threading import Thread

    send_every = 10
    port = 6009

    f = FinanceWorker2(send_every=10)
    f.start()
    time.sleep(1)
    # s = StreamWorker(use_processes=True)
    # s.start()
    # time.sleep(1)

    # p = PreProcessWorker()
    # p.start()
    # port += 1
    # time.sleep(1)
    #
    # st = SentimentWorker(send_every=send_every)
    # st.start()
    # port += 1
    # time.sleep(1)

    c = Client(('localhost', port), authkey=b'veryscrape')
    # items = []
    # old_items = []
    # while True:
    #     if len(items) == 110:
    #         alive = sum((item.content != 0 and item.content != old.content) for item, old in zip(items, old_items))
    #         total = sum(item.content != 0 for item in items)
    #         print(alive, total)
    #         old_items = items
    #         items = []
    #     i = c.recv()
    #     if i.source == 'reddit':
    #         items.append(i)
    #
    # topics = Producer.load_query_dictionary('query_topics.txt')
    # auths = Producer.load_authentications('twitter.txt', topics)
    # l = asyncio.get_event_loop()
    # q = Queue()
    #
    # async def run():
    #     jobs = []
    #     n = 20
    #     ct = 0
    #     for topic, qs in topics.items():
    #         #for qr in qs:
    #         if ct >= n:
    #             print('Finished batch')
    #             #time.sleep(5)
    #             ct = 0
    #         jobs.append(asyncio.ensure_future(TweetStream(auths[topic], topic, qs, q).stream()))
    #         ct += 1
    #     await asyncio.wait(jobs)
    #
    def prnt():
        items = defaultdict(list)
        start = time.time()
        iss = time.time()
        while True:
            i = c.recv()
            print(i)
            # items[i.topic].append(i)
            # if time.time() - start > 5:
            #     print(len(items))
            #     start = time.time()
            # if time.time() - iss > 60:
            #     items = defaultdict(list)
            #     iss = time.time()

    Thread(target=prnt).start()
    # l.run_until_complete(run())
