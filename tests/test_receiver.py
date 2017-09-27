import json
import unittest

import requests


class TestReceiver(unittest.TestCase):

    def test_basic_send(self):
        r = requests.get('http://192.168.1.53:9999',
                         json=json.dumps({'content': 'Receiver test', 'topic': 'TEST', 'source': 'test'}))
        assert r.status_code == 200, 'Request returned error!'

if __name__ == '__main__':
    unittest.main()
