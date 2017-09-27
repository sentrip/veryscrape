from veryscrape import SearchClient


class Reddit(SearchClient):
    base_url = 'https://oauth.reddit.com/r/'
    token_url = 'https://www.reddit.com/api/v1/access_token'
    rate_limit = 60

    def __init__(self, auth):
        super(Reddit, self).__init__()
        self.client, self.secret = auth

    async def comment_stream(self, track=None, topic=None, duration=3600, use_proxy=False, **reddit_params):

        url = '{}/hot.json?{}&raw_json=1&limit=30'.format(track, urlencode(params))
        comment_url = self.BASE + '%s/comments/{}.json?%s&raw_json=1&limit=10000&depth=10' % (query, urlencode(params))
        j = await self.fetch_json(link_url)
        links = {i['data']['id']: i['data']['created_utc'] for i in j['data']['children']}
        last_id = j['data']['after']
        for link in links:


            with suppress(KeyError):


                j = await self.fetch_json(comment_url.format(link))
                for c in j[1]['data']['children']:
                    if c['kind'] == 't1':
                        if c['data']['id'] not in self.seen[query]:
                            self.queue.put(Item(c['data']['body_html'], self.topic, 'reddit'))
                            self.seen[query].add(c['data']['id'])
        return last_id
