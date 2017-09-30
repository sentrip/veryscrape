import unittest

import requests


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

if __name__ == '__main__':
    unittest.main()
