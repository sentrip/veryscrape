import pytest
import re
import random

from veryscrape.items import Item
from veryscrape.process import *

# size of data used for tests makes using parametrize too slow


def test_remove_links(static_data):
    url_lines = static_data('url_lines')
    for url_line in url_lines:
        clean = remove_urls(url_line)
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


# Ignore DeprecationWarning caused by newspaper library
@pytest.mark.filterwarnings('ignore::DeprecationWarning')
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


def test_classify_text():
    text = 'apple apple microsoft apple microsoft'
    result = classify_text(text, {'AAPL': ['apple'], 'MS': ['microsoft']})
    assert result == 'AAPL', 'Did not correctly classify text'


def test_register():
    register('test1', lambda t: t.replace('1', '2'))
    result = clean_item(Item('aa1a1a1', source='test1'))
    assert result.content == 'aa2a2a2', 'Did not use correct clean function'


def test_unregister():
    unregister('twitter', clean_general)
    text = '#stuffandthings hi my name is !@#$#@!@#$'
    item = Item(text, source='twitter')
    result = clean_item(item)
    assert result.content == ' stuffandthings  hi my name is !@#$#@!@#$', \
        'Incorrectly cleaned result'

    unregister('twitter')
    result = clean_item(item)
    assert result.content == text, 'Incorrectly cleaned result'

    unregister('*')
    item.source = 'article'
    result = clean_item(item)
    assert result.content == text, \
        'Applied cleaning functions after global de-register'


def test_custom_clean_func():
    import veryscrape.process
    veryscrape.process.register('custom', lambda s: s.replace('@123@ ', ''))
    item = Item('data @123@ data', '', 'custom')
    cleaned = veryscrape.process.clean_item(item)
    assert cleaned.content == 'data data', 'Did not clean custom item correctly'
