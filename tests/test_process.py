import re
import random

from veryscrape.items import Item
from veryscrape.process import remove_links, \
    clean_article, clean_reddit_comment, clean_tweet, clean_item

# size of data used for tests makes using parametrize too slow


def test_remove_links(static_data):
    url_lines = static_data('url_lines')
    for url_line in url_lines:
        clean = remove_links(url_line)
        assert 'http' not in clean, \
            'FAIL: \n\toriginal: %s \n\tcleaned: %s\n' % (url_line, clean)


def test_clean_tweet(static_data):
    """Test string cleaning for tweets"""
    tweets = static_data('tweets')
    at_reg = re.compile('@[a-zA-Z0-9_]{2,}')
    rt_reg = re.compile('\x20?RT\x20?:?')
    hash_reg = re.compile('@[a-zA-Z0-9_]{2,}')

    for i, tweet in enumerate(random.sample(tweets, 10000)):
        new_tweet = clean_tweet(tweet)
        assert not re.findall(rt_reg, new_tweet), 'RT was not successfully cleaned\n{}, {}'.format(i, tweet)
        assert not re.findall(at_reg, new_tweet), '@ was not successfully cleaned\n{}, {}'.format(i, tweet)
        assert not re.findall(hash_reg, new_tweet), '# was not successfully cleaned\n{}, {}'.format(i, tweet)


def test_clean_reddit(static_data):
    """Test string cleaning for reddit comments"""
    reg = re.compile(r'r/[^ ]*]')
    comments = static_data('comments')
    for cm in comments:
        c = clean_reddit_comment(cm)
        assert not re.findall(reg, c), 'Subreddit was not successfully cleaned, {}'.format(c)
        assert all(j not in c for j in ['[deleted]', '[removed]', '[not found]']), 'Incorrect comment returned!'


def test_clean_article_blog(static_data):
    """Test string cleaning for articles and blog posts"""
    htmls = static_data('htmls')
    for h in htmls:
        art = clean_article(h)
        assert '</' not in art, 'Html was not correctly converted into article!'

    assert clean_article('') == '', 'Did not return on failed clean'


def test_clean_item(static_data):
    """Test string cleaning for general - newline normalization and http link removal"""
    alls = [Item(i, '', 'reddit') for i in static_data('comments')] + \
          [Item(i, '', 'twitter') for i in random.sample(static_data('tweets'), 10000)] + \
          [Item(i, '', 'article') for i in static_data('htmls')]

    for q in alls:
        res = clean_item(q).content
        assert all(j not in res for j in ['\n', '\r', '\t']), \
            'Item has unnormalized new lines, {}'.format(res)
        assert ('http:' not in res and 'https:' not in res), \
            'Item has an uncleaned link! {}'.format(res)
