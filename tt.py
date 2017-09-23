import asyncio

from base import Producer
from extensions.twitter import *

topics = Producer.load_query_dictionary('query_topics.txt')
auths = Producer.load_authentications('twitter.txt', topics)


async def run(q):
    client = AsyncOAuth(*auths[q][:2], oauth_token=auths[q][2], oauth_token_secret=auths[q][3],
                          base_url='https://stream.twitter.com/1.1/')
    params = {'language': 'en', 'track': 'facebook'}
    try:
        stream = await client.request('POST', 'statuses/filter.json?', params=params)
        #print(stream.status)
        # buf = b''
        # while b'\n' not in buf:
        #     chunk = await stream.content.read(64)
        #     buf += chunk
        # ind = buf.find(b'\n')
        # status, buf = buf[:ind], buf[ind + 1:]
        # txt = re.sub(r'[^\x00-\x7f]', '', unquote_plus(re.search(r'"text":"(.*?)"', str(status)).group(1)))
        # print(txt)
    except:
        return False
    client.close()
    if stream.status == 200:
        return True
    else:
        return False

async def run_all(n=10):
    tpcs = iter(topics)
    finished = False
    while not finished:
        cn, qs = 0, []
        while cn < n:
            try:
                qs.append(next(tpcs))
            except StopIteration:
                break
            cn += 1
        successes = await asyncio.gather(*[run(i) for i in qs])
        for success, q in zip(successes, qs):
            if not success:
                print(q, success, *auths[q][2:])
            else:
                print(q, 'Success!')
        await asyncio.sleep(5)


l = asyncio.get_event_loop()
#l.run_until_complete(run('FB'))
l.run_until_complete(run_all(20))
