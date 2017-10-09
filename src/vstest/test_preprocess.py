import os
import random
import re
import unittest
from collections import defaultdict
from multiprocessing import Queue

import numpy as np

from veryscrape.api import Item
from veryscrape.preprocess import PreProcessor


def load_data(pth):
    tweets = []
    with open(pth + '/tweets.txt', encoding='utf-8', errors='replace') as f:
        for i, ln in enumerate(f):
            tweets.append(ln.strip('\n'))
    comments = []
    with open(pth + '/reddit_comments.txt', encoding='utf-8', errors='replace') as f:
        for ln in f:
            comments.append(ln.strip('\n'))
    sep = '|S|P|E|C|I|A|L|S|E|P|'
    with open(pth + '/htmls.txt', encoding='utf-8', errors='replace') as f:
        data = f.read()
        htmls = data.split(sep)
    return tweets, comments, htmls


class TestPreProcess(unittest.TestCase):
    current = os.getcwd()[:os.getcwd().find('src')] + 'src/vstest/'
    input_queue = Queue()
    output_queue = Queue()
    client = PreProcessor(input_queue, output_queue)
    client.vocab = client.load_vocab()
    tweets, comments, htmls = load_data(current + 'data')

    def tearDown(self):
        while not self.input_queue.empty():
            _ = self.input_queue.get()
        while not self.output_queue.empty():
            _ = self.output_queue.get()

    def test_load_vocab(self):
        v = self.client.load_vocab()
        assert isinstance(v, defaultdict), "Incorrect dictionary format returned"
        assert len(v) == 250000, 'Vocab of incorrect length'
        assert v['</s>'] == 0, 'Vocab not correctly loaded'

    def test_clean_tweet(self):
        """Test string cleaning for tweets"""
        at_reg = re.compile('@[a-zA-Z0-9_]{2,}')
        rt_reg = re.compile('\x20?RT\x20?:?')
        hash_reg = re.compile('@[a-zA-Z0-9_]{2,}')

        for i, tweet in enumerate(random.sample(self.tweets, 10000)):
            new_tweet = self.client.clean_tweet(Item(tweet, '', '')).content
            assert not re.findall(rt_reg, new_tweet), 'RT was not successfully cleaned\n{}, {}'.format(i, tweet)
            assert not re.findall(at_reg, new_tweet), '@ was not successfully cleaned\n{}, {}'.format(i, tweet)
            assert not re.findall(hash_reg, new_tweet), '# was not successfully cleaned\n{}, {}'.format(i, tweet)

    def test_clean_reddit(self):
        """Test string cleaning for reddit comments"""
        for cm in self.comments:
            c = self.client.clean_reddit_comment(Item(cm, '', ''))
            assert 'r/' not in c, 'Subreddit was not successfully cleaned, {}'.format(c)
            assert all(j not in c for j in ['[deleted]', '[removed]', '[not found]']), 'Incorrect comment returned!'

    def test_clean_article_blog(self):
        """Test string cleaning for articles and blog posts"""
        for h in self.htmls:
            art = self.client.clean_article(Item(h, '', ''))
            assert '</' not in art.content, 'Html was not correctly converted into article!'

    def test_clean_general(self):
        """Test string cleaning for general - newline normalization and http link removal"""
        alls = [self.client.clean_reddit_comment(Item(i, '', '')) for i in self.comments] + \
              [self.client.clean_tweet(Item(i, '', '')) for i in random.sample(self.tweets, 10000)] + \
              [self.client.clean_article(Item(i, '', '')) for i in self.htmls]
        for q in alls:
            i = self.client.clean_general(q)
            assert all(j not in i.content for j in ['\n', '\r', '\t']), \
                'Item has unnormalized new lines, {}'.format(i.content)
            assert ('http:' not in i.content and 'https:' not in i.content), \
                'Item has an uncleaned link! {}'.format(i.content)

    def test_feature_convert(self):
        """Test string conversion to feature vector"""
        sent = 'hello this is my sentence, i would like to test the feature conversion! please give me a good matrix.'
        item = self.client.feature_convert(Item(sent, '', ''))
        assert item.content.shape == (150, ), "Incorrect shape of features returned, {}".format(item.content.shape)
        assert np.sum(np.sum(item.content, axis=0)) > 0, 'No words were found'
