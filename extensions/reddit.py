import asyncio
import time
from collections import defaultdict
from urllib.parse import urlencode

import aiohttp

from base import Item


async def new_session(key, secret):
    token_url = 'https://www.reddit.com/api/v1/access_token'
    async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(key, secret), headers={'user-agent': 'test app'}) as client:
        async with client.post(token_url, data={'grant_type': 'client_credentials'}) as response:
            j = await response.json()
    return aiohttp.ClientSession(headers={'user-agent': 'test app',
                                          'Authorization': 'bearer ' + j['access_token']}), time.time() + 3598


async def links_from_subreddit(sess, subreddit, **params):
    req_url = 'https://oauth.reddit.com/r/{}/hot.json?{}&raw_json=1&limit=30'.format(subreddit, urlencode(params))
    async with sess.get(req_url) as response:
        j = await response.json()
    try:
        links = {i['data']['id']: i['data']['created_utc'] for i in j['data']['children']}
        return links, j['data']['after']
    except KeyError:
        return {}, None


async def comments_from_link(sess, subreddit, link_id, **params):
    req_url = 'https://oauth.reddit.com/r/{}/comments/{}.json?{}&raw_json=1&limit=10000&depth=10'.format(subreddit, link_id, urlencode(params))
    async with sess.get(req_url) as response:
        j = await response.json()
    comments, ids = [], []
    for c in j[1]['data']['children']:
        if c['kind'] == 't1':
            comments.append(c['data']['body_html'])
            ids.append(c['data']['id'])
    return comments, ids


async def reddit(parent, topic, query):
    links = {}
    seen_comments = set()
    link_last_ids = defaultdict(int)
    link_last_id = None
    start_time = time.time()
    sess, expiry_time = await new_session(*parent.reddit_authentications[topic])
    while parent.running:
        try:
            params = {}
            if link_last_id:
                params['after'] = link_last_id
            if link_last_ids[link_last_id]:
                params['count'] = link_last_ids[link_last_id]

            if parent.reddit_rate_limit[topic] <= 0:
                await asyncio.sleep(2)
            new_links, new_id = await links_from_subreddit(sess, query, **params)
            parent.reddit_rate_limit[topic] -= 1

            links.update(new_links)
            link_last_id = new_id or link_last_id
            link_last_ids[link_last_id] += len(new_links)

            for link, time_created in list(links.items()):
                if time.time() - time_created >= 2 * 24 * 3600:
                    del links[link]
                    continue

                if parent.reddit_rate_limit[topic] <= 0:
                    await asyncio.sleep(2)
                comments, ids = await comments_from_link(sess, query, link)
                parent.reddit_rate_limit[topic] -= 1

                for c, cid in zip(comments, ids):
                    if cid not in seen_comments:
                        parent.result_queue.put(Item(c, topic, 'reddit'))
                seen_comments.update(set(ids))

                now = time.time()
                if now - start_time >= 1:
                    parent.reddit_rate_limit[topic] += 1
                    start_time += 1
                if time.time() >= expiry_time:
                    sess, expiry_time = await new_session(*parent.reddit_authentications[topic])
        except Exception as e:
            print('Reddit', repr(e))
