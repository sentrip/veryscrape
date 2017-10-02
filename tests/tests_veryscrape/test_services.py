import unittest
import requests
import json

from vs import load_query_dictionary


class TestProxy(unittest.TestCase):
    server = 'http://192.168.0.100:9999'

    def test_proxy_request_no_params(self):
        r = requests.get(self.server)
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

    def test_proxy_request_correct_params(self):
        r = requests.get(self.server, params={'speed': 100, 'https': 1})
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

    def test_proxy_request_incorrect_params(self):
        r = requests.get(self.server, params={'a': 1})
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'


class TestAPI(unittest.TestCase):
    server = 'http://192.168.0.100:1111'

    def test_api_request_no_params(self):
        r = requests.get(self.server)
        assert r.status_code == 404, 'No params request for api key failed'

    def test_api_request_correct_params(self):
        r = requests.get(self.server, params={'type': 'twingly'})
        assert r.status_code == 200, 'Correct request for api key failed'
        assert 'auth' in r.text, 'No authentication in return data'

    def test_api_request_incorrect_params(self):
        r = requests.get(self.server, params={'a': 1})
        assert r.status_code == 404, 'Incorrect request for api key failed'


class TestReceiver(unittest.TestCase):

    def test_correct_send(self):
        correct = ['article', 'blog', 'reddit', 'twitter', 'stock']
        d = {k: {c: 0.1 for c in load_query_dictionary('query_topics')} for k in correct}
        r = requests.post('http://192.168.1.53:9999',
                          data=json.dumps(d))
        assert r.status_code == 200, 'Request returned error!'

    def test_incorrect_send(self):
        r = requests.post('http://192.168.1.53:9999', data={'a': 1})
        assert r.status_code == 404, 'Request returned error!'


if __name__ == '__main__':
    unittest.main()
