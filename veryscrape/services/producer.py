import asyncio
from threading import Thread
from multiprocessing import Process

import veryscrape.extensions as vs
from veryscrape import synchronous, get_auth, load_query_dictionary


# class Producer:
#     def __init__(self):
#         self.url_queue = asyncio.Queue()
#         self.result_queue = asyncio.Queue()
#         self.preprocess_queue = Queue()
#         # self.sentiment_queue = Queue()
#         self.output_queue = asyncio.Queue()
#         self.topics = load_query_dictionary('query_topics')
#
#     async def run_crawl(self, client_class, client_args, use_proxy=False):
#         jobs = []
#         clients = []
#         kwargs = {'use_proxy': True} if use_proxy else {}
#         for i, (topic, qs) in enumerate(self.topics.items()):
#             client = client_class(*client_args[i])
#             clients.append(client)
#             for q in qs:
#                 jobs.append(client.stream(q, topic, duration=3600, **kwargs))
#         await asyncio.gather(*jobs)
#         for client in clients:
#             await client.close()
#
#     async def finance_crawl(self, use_proxy=False):
#         finance = vs.Finance(self.output_queue)
#         jobs = []
#         for topic in self.topics:
#             jobs.append(finance.finance_stream(topic, duration=10800, use_proxy=use_proxy))
#         await asyncio.gather(*jobs)
#
#     @staticmethod
#     def run_in_loop(job):
#         policy = asyncio.get_event_loop_policy()
#         policy.set_event_loop(policy.new_event_loop())
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(job)
#
#     @synchronous
#     async def run(self, n_processes=2):
#         pool = ProcessPoolExecutor(n_processes)
#         auths = {
#             'twitter': await get_auth('twitter'),
#             'twingly': (await get_auth('twingly'))[1][0],
#             'reddit': await get_auth('reddit')
#         }
#         downloader = vs.Download(self.url_queue, self.result_queue)
#         preprocessor = vs.PreProcessor(self.result_queue, self.preprocess_queue, pool)
#         g_args, b_args = [[self.url_queue]]*len(self.topics), [(auths['twingly'], self.url_queue)] * len(self.topics)
#         r_args = list(zip(auths['reddit'], [self.result_queue]*len(self.topics)))
#         tw_args = list(zip(auths['twitter'], [self.result_queue]*len(self.topics)))
#         jobs = [downloader.stream(),
#                 self.finance_crawl(use_proxy=True),
#                 self.run_crawl(vs.Twingly, b_args),
#                 self.run_crawl(vs.Reddit, r_args),
#                 self.run_crawl(vs.Google, g_args, use_proxy=True),  # , use_proxy=True),
#                 self.run_crawl(vs.Twitter, tw_args, use_proxy=True)]
#         preprocessor.start()
#         for job in jobs:
#             Process(target=self.run_in_loop, args=(job,)).start()
#
#         while True:
#             try:
#                 if not self.preprocess_queue.empty():
#                     item = self.preprocess_queue.get_nowait()
#                     print(item)
#             except KeyboardInterrupt:
#                 break
#         print('done!')


class Runner(Thread):
    def __init__(self, f):
        super(Runner, self).__init__()
        self.f = f

    def run(self):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.f())


class Producer:
    def __init__(self):
        self.topics = load_query_dictionary('query_topics')
        self.url_queue = asyncio.Queue()
        self.result_queue = asyncio.Queue()

    @staticmethod
    def run_in_loop(job):
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(job))

    async def twingly(self):
        auth = await get_auth('twingly')
        client = vs.Twingly(auth[0][0], self.url_queue)
        jobs = []
        args = []
        for topic, qs in self.topics.items():
            for q in qs:
                args.append((q, topic))
                jobs.append(asyncio.ensure_future(client.blog_stream(q, topic)))
        while True:
            for job in jobs:
                if job.done():
                    if job.exception():
                        print(job.exception())
                    i = jobs.index(job)
                    job = asyncio.ensure_future(client.blog_stream(*args[i]))
                    jobs[i] = job
                await asyncio.sleep(0)

    async def main_loop(self):
        Runner(self.twingly).start()

        down = vs.Download(self.url_queue, self.result_queue)
        Runner(down.stream).start()

        while True:
            item = await self.result_queue.get()
            print(item)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_loop())

if __name__ == '__main__':
    producer = Producer()
    producer.run()

