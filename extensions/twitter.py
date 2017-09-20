import asyncio
import json
import re
import time
from contextlib import suppress

from base import AsyncOAuth
from base import Item


async def async_stream_read_loop(parent, stream, topic, chunk_size=1024):
    buffer = b''
    charset = stream.headers.get('content-type', default='')
    enc_search = re.search('charset=(?P<enc>\S*)', charset)
    encoding = enc_search.group('enc') if enc_search is not None else 'utf-8'

    while True:
        try:
            chunk = await stream.content.read(chunk_size)
        except Exception as e:
            print('' + repr(e))
            break
        if not chunk:
            break
        buffer += chunk
        ind = buffer.find(b'\n')
        if ind > -1:
            status, buffer = buffer[:ind], buffer[ind+1:]
            with suppress(json.JSONDecodeError):
                s = json.loads(status.decode(encoding))
                await asyncio.sleep(0)
                if 'limit' in s:
                    sleep_time = (float(s['limit']['track']) + float(s['limit']['timestamp_ms']))/1000 - time.time()
                    await asyncio.sleep(sleep_time)
                else:
                    parent.result_queue.put(Item(s['text'], topic, 'twitter'))


async def twitter(parent, topic, query):
    """Asynchronous twitter stream - streams tweets for provided query, topic is used for categorization"""
    retry_time_start = 5.0
    retry_420_start = 60.0
    retry_time_cap = 320.0
    snooze_time_step = 0.25
    snooze_time_cap = 16
    retry_time = retry_time_start
    snooze_time = snooze_time_step
    auth = parent.twitter_authentications[topic]
    client = AsyncOAuth(*auth, 'https://stream.twitter.com/1.1/')
    p = {'language': 'en', 'track': query}
    while parent.running:
        # get proxy
        try:
            stream = await client.request('POST', 'statuses/filter.json', params=p)
        except Exception as e:
            print('Twitter', repr(e))
            await asyncio.sleep(retry_time * 2)
            client = AsyncOAuth(*auth, 'https://stream.twitter.com/1.1/')
            continue

        if stream.status != 200:
            if stream.status == 420:
                retry_time = max(retry_420_start, retry_time)
            await asyncio.sleep(retry_time)
            retry_time = min(retry_time * 2., retry_time_cap)
        else:
            retry_time = retry_time_start
            snooze_time = snooze_time_step
            await async_stream_read_loop(parent, stream, topic)

        await asyncio.sleep(snooze_time)
        snooze_time = min(snooze_time + snooze_time_step, snooze_time_cap)
    client.close()
