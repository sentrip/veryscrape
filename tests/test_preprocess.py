import unittest
from multiprocessing import Queue

from veryscrape import BASE_DIR
from veryscrape.extensions.preprocess import PreProcessor

TWEETS = []
with open(BASE_DIR + '/tweets.txt', encoding='utf-8', errors='replace') as f:
    for i, ln in enumerate(f):
        if i < 100000:
            TWEETS.append(ln.strip('\n'))
        else:
            break


class TestPreProcess(unittest.TestCase):
    input_queue = Queue()
    output_queue = Queue()

    def setUp(self):
        self.client = PreProcessor(self.input_queue, self.output_queue)

    def tearDown(self):
        while not self.input_queue.empty():
            _ = self.input_queue.get()
        while not self.output_queue.empty():
            _ = self.output_queue.get()

    # def test_load_vocab(self):
    #     v = self.client.load_vocab()
    #     assert isinstance(v, defaultdict), "Incorrect dictionary format returned"
    #     assert len(v) == 1000000, 'Vocab of incorrect length'
    #     assert v['</s>'] == 2, 'Vocab not correctly loaded'

    # def test_load_bigram(self):
    #     v = self.client.load_ngram()
    #     assert v[['new', 'york']] == ['new_york'], "Bigram does not correctly convert new york"

    # def test_clean_tweet(self):
    #     """Test string cleaning for tweets"""
    #     at_reg = re.compile('@[a-zA-Z0-9_]{2,}')
    #     rt_reg = re.compile('\x20?RT\x20?:?')
    #     hash_reg = re.compile('@[a-zA-Z0-9_]{2,}')
    #
    #     for i, tweet in enumerate(random.sample(TWEETS, 10000)):
    #         new_tweet = self.client.clean_tweet(Item(tweet, '', '')).content
    #         assert not re.findall(rt_reg, new_tweet), 'RT was not successfully cleaned\n{}, {}'.format(i, tweet)
    #         assert not re.findall(at_reg, new_tweet), '@ was not successfully cleaned\n{}, {}'.format(i, tweet)
    #         assert not re.findall(hash_reg, new_tweet), '# was not successfully cleaned\n{}, {}'.format(i, tweet)
    #

    def test_clean_reddit(self):
        pass

    def test_clean_article_blog(self):
        pass

    def test_clean_general(self):
        pass

    def test_feature_convert(self):
        pass

if __name__ == '__main__':
    unittest.main()
