import unittest
from multiprocessing import Queue

from veryscrape import synchronous, Item, load_query_dictionary
from veryscrape.extensions.download import Download


class TestDownload(unittest.TestCase):
    url_queue = Queue()
    result_queue = Queue()

    def setUp(self):
        self.topics = load_query_dictionary('query_topics')

    @synchronous
    async def test_download_small_url_set(self):
        urls = ['http://fortune.com/2017/09/29/apple-mac-update-security-sierra-firmware/',
                'https://techcrunch.com/2017/09/29/apple-quietly-acquires-computer-vision-startup-regaind/',
                'http://www.businessinsider.com/apple-considers-ditching-intel-making-own-chip-for-mac-laptops-report-2017-9']
        down = Download(self.url_queue, self.result_queue)
        for u in urls:
            self.url_queue.put(Item(u, 'AAPL', 'article'))
        await down.stream(duration=1)
        assert self.url_queue.empty(), 'Url queue was not purged'
        res = self.result_queue.get_nowait()
        assert res, 'Result queue is empty'
        assert res.content.startswith('<'), 'Incorrect response from download'
        await down.close()

if __name__ == '__main__':
    unittest.main()
