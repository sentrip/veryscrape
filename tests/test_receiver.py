import unittest

import requests


class TestReceiver(unittest.TestCase):

    def test_correct_send(self):
        r = requests.post('http://192.168.1.53:9999',
                          data={'content': 'Receiver test', 'topic': 'TEST', 'source': 'test'})
        assert r.status_code == 200, 'Request returned error!'

    def test_incorrect_send(self):
        r = requests.post('http://192.168.1.53:9999', data={'a': 1})
        assert r.status_code == 404, 'Request returned error!'

if __name__ == '__main__':
    unittest.main()
