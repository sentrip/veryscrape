import asyncio
import time

from veryscrape import SearchClient, ReadBuffer, async_run_forever


class Twitter(SearchClient):
    base_url = 'https://stream.twitter.com/1.1/'

    # Rate limits
    retry_420 = 60
    snooze_time = 0.25

    def __init__(self, auth):
        super(Twitter, self).__init__()
        self.client, self.secret, self.token, self.token_secret = auth

    async def filter_stream(self, track=None, topic=None, duration=3600, use_proxy=False):
        start_time = time.time()
        raw = await self.request('POST', 'statuses/filter.json', oauth=1, stream=True,
                                 params={'langauge': 'en', 'track': track},
                                 use_proxy={'speed': 100, 'https': 1, 'post': 1} if use_proxy else None)
        if raw.status == 420:
            self.snooze_time += 0.5
            await asyncio.sleep(self.retry_420)
        elif raw.status != 200:
            self.failed = True
            self.snooze_time += 0.5
            raise ValueError('Incorrect stream response!')
        else:
            self.failed = False
            self.snooze_time = 0.25
            buffer = ReadBuffer(raw)
            async for status in buffer:
                await self.send_item(status['text'], topic, 'twitter')
                await asyncio.sleep(self.snooze_time)

                if time.time() - start_time >= duration:
                    break

    @async_run_forever
    async def stream(self, track=None, topic=None, duration=3600, use_proxy=False):
        await self.filter_stream(track, topic, duration, use_proxy)
