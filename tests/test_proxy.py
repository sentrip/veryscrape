import json
import unittest

import requests


class TestProxy(unittest.TestCase):

    def test_proxy_request_no_params(self):
        r = requests.get('http://192.168.0.100:9999')
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

    def test_proxy_request_regular_params(self):
        r = requests.get('http://192.168.0.100:9999', json=json.dumps({'speed': 150, 'https': '1'}))
        assert r.status_code == 200, 'Request for proxy failed'
        assert r.text.startswith('http'), 'Invalid proxy returned'

if __name__ == '__main__':
    unittest.main()
