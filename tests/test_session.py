import time
import pytest
from veryscrape.session import fetch


@pytest.mark.asyncio
async def test_get_attributes(patched_session):
    async with patched_session() as sess:
        assert len(sess.cookie_jar) == 0, \
            "Did not get correct session attribute from _session"
        assert sess.limiter.rate_limit_period == 60, \
            "Did not get correct session attribute"


@pytest.mark.asyncio
async def test_user_agent(patched_session):
    class Persist(patched_session):
        user_agent = 'test'
        persist_user_agent = True

    class Change(patched_session):
        user_agent = 'test'
        persist_user_agent = False

    for klass, cond in zip([Persist, Change], [True, False]):
        async with klass() as sess:
            for _ in range(2):
                async with sess.request('GET', 'user') as resp:
                    assert (resp._body == 'test') == cond, 'Incorrect user agent'


@pytest.mark.asyncio
async def test_proxy_request(patched_session):
    async def g(scheme=None):
        return type('', (), {'host': '127.0.0.1', 'port': 5000})
    pool = type('', (), {'get': g})
    data = []
    async with patched_session(proxy_pool=pool) as sess:
        async with sess.get('http://url.com', _data_collect=data) as resp:
            pass
    assert data[0][2]['proxy'] == 'http://127.0.0.1:5000', \
        'Did not use correct proxy'


@pytest.mark.asyncio
async def test_rate_limited(assert_rate_limited, patched_session):
    class TestRateLimitedSession(patched_session):
        rate_limit_period = 0.1
        rate_limits = {'/users': 5}

    async with TestRateLimitedSession() as sess:
        await assert_rate_limited(sess, 5, 'test/users', 0, 2)
        await assert_rate_limited(sess, 5, 'test/users',
                                  sess.rate_limit_period / sess.rate_limits['/users'], 2)


@pytest.mark.asyncio
async def test_rate_limited_nested_and_global(assert_rate_limited, patched_session):
    class TestRateLimitedSession(patched_session):
        rate_limit_period = 0.1
        rate_limits = {'/users': {'1': 5, '2': 10}, '*': 20}

    async with TestRateLimitedSession() as sess:
        await assert_rate_limited(sess, 5, 'test/users/1', 0, 2)
        await assert_rate_limited(sess, 5, 'test/users/1',
                                  sess.rate_limit_period / sess.rate_limits['/users']['1'], 2)


@pytest.mark.asyncio
async def test_overridden_request(assert_rate_limited, patched_session):
    calls = []

    class TestRateLimitedSession(patched_session):
        rate_limit_period = 0.01
        rate_limits = {'/users': 5}

        async def _request(self, *args, **kwargs):
            calls.append(args)
            return await super(TestRateLimitedSession, self)._request(*args, **kwargs)

    async with TestRateLimitedSession() as sess:
        await assert_rate_limited(sess, 5, 'test/users', 0, 2)
        assert len(calls) == 5, 'Did not call custom request'
        await assert_rate_limited(sess, 5, 'test/users',
                                  sess.rate_limit_period / sess.rate_limits['/users'], 3)


@pytest.mark.asyncio
async def test_oauth1(patched_oauth1):
    async with patched_oauth1('c', 's', 'tk', 'ts') as client:
        _, params, _ = await client.patcher.patch_request('GET', 'test', {}, {})
        required_params = [
            'oauth_consumer_key', 'oauth_signature_method', 'oauth_nonce',
            'oauth_timestamp', 'oauth_version', 'oauth_signature'
        ]
        for p in required_params:
            assert params.get(p), 'Did not set %s in params' % p


@pytest.mark.asyncio
async def test_oauth1_request(patched_oauth1):
    async with patched_oauth1('', '', '', '') as sess:
        sess.base_url = 'http://testurl.com/'
        res = await fetch('GET', 'stuff.html', session=sess, params={})
        assert res.startswith('<!doctype'), \
            'Did not correctly request with base url'


@pytest.mark.asyncio
async def test_oauth2_no_token(patched_oauth2):
    """Test oauth2 token retrieval when new token is required"""
    async with patched_oauth2(
        'VbKYA5v77UPooA', 'OZan8kt5EluEZ0pXpMbmtLoPTgk', 'oauth2'
    ) as client:
        headers = await client.patcher.auth_token()
        assert headers == {'Authorization': 'bearer ' + 'abc_test_abc'}, \
            'Incorrect headers returned'


@pytest.mark.asyncio
async def test_oauth2_token_exists_not_expired(patched_oauth2):
    """Test oauth2 token retrieval if current token is unexpired"""
    async with patched_oauth2(
        'VbKYA5v77UPooA', 'OZan8kt5EluEZ0pXpMbmtLoPTgk', 'oauth2'
    ) as client:
        client.patcher.token = 'hello'
        client.patcher.token_expiry = time.time() + 7200
        _, _, kwargs = await client.patcher.patch_request('', '', {}, {})
        headers = kwargs.pop('headers')
        assert headers == {'Authorization': 'bearer ' + 'hello'}, \
            'Incorrect headers returned'


@pytest.mark.asyncio
async def test_oauth2_token_exists_expired(patched_oauth2):
    """Test oauth2 token retrieval when current token has expired"""
    async with patched_oauth2(
        'VbKYA5v77UPooA', 'OZan8kt5EluEZ0pXpMbmtLoPTgk', 'oauth2'
    ) as client:
        client.patcher.token = 'hello'
        client.patcher.token_expiry = time.time() - 10
        _, _, kwargs = await client.patcher.patch_request('', '', {}, {})
        headers = kwargs.pop('headers')
        assert client.patcher.token != 'hello', 'Token was not updated when it should have been'
        assert headers == {'Authorization': 'bearer ' + client.patcher.token}, \
            'Incorrect headers returned'


@pytest.mark.asyncio
async def test_fetch(patched_aiohttp):
    res = await fetch('GET', 'fetch')
    assert res == 'test', 'Did not get correct data with fetch'


@pytest.mark.asyncio
async def test_fetch_stream_func(patched_aiohttp):
    data = []
    await fetch('GET', 'fstream', stream_func=lambda l: data.append(l))
    assert data == [b'test%d' % i for i in range(len(data))], \
        "Did not correctly execute stream function with fetch"


@pytest.mark.asyncio
async def test_fetch_http_error(patched_aiohttp):
    res = await fetch('GET', 'error')
    assert res is '', 'Returned data for failed request'


@pytest.mark.asyncio
async def test_fetch_http_exception(patched_aiohttp):
    res = await fetch('GET', 'exception')
    assert res is '', 'Returned data for failed request'


@pytest.mark.asyncio
async def test_fetch_custom_session(patched_session):
    async with patched_session() as sess:
        res = await fetch('GET', 'fetch', session=sess)
        assert res == 'test', 'Did not get correct data with fetch'
        assert not sess.closed, "Closed session passed into fetch"

