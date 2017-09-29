import unittest

import requests


class TestProxy(unittest.TestCase):
    server = 'http://192.168.0.100:9999'

    def test_proxy_request_no_params(self):
        r = requests.get(self.server)
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

    def test_proxy_request_correct_params(self):
        r = requests.get(self.server, params={'speed': 150, 'https': 1})
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

    def test_proxy_request_incorrect_params(self):
        r = requests.get(self.server, params={'a': 1})
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

if __name__ == '__main__':
    unittest.main()
