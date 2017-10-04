# todo write tests for scrape!
# if __name__ == '__main__':
#     import asyncio
#     from veryscrape.request import get_auth
#     from vstest import synchronous
#     q = asyncio.Queue()
#     q2 = asyncio.Queue()
#     @synchronous
#     async def a():
#         ass = await get_auth('twingly')
#         return ass[0]
#
#     async def prnt():
#         while True:
#             i = await q.get()
#             print(i)
#     ath = a()[0]
#     b = Finance()
#     l = asyncio.get_event_loop()
#     l.run_until_complete(asyncio.gather(b.scrape('FB', 'FB', q), prnt()))#, download(q, q2)))
