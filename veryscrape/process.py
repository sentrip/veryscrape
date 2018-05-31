from collections import defaultdict, Counter
from newspaper import fulltext
from xml.sax.saxutils import unescape
import lxml.html
import re
import threading

from .items import Item

_clean_functions = defaultdict(list)
_mutex = threading.Lock()


def register(name, *funcs):
    """
    Register cleaning function so it is run automatically
    on items with source 'name'
    :param name: name of data source (e.g. 'twitter')
    :param funcs: cleaning functions to apply
    """
    with _mutex:
        _clean_functions[name].extend(funcs)


def unregister(name, *funcs):
    """
    Unregister a function registered with 'veryscrape.process.register'
    :param name: name of data source (e.g. 'twitter')
    :param funcs: cleaning functions to remove
    """
    with _mutex:
        if name == '*':
            _clean_functions.clear()
        elif not funcs:
            del _clean_functions[name]
        else:
            for func in funcs:
                _clean_functions[name].remove(func)


def clean_article(content):
    """Converts html text into article text"""
    result = ''
    try:
        result = fulltext(content)
    finally:  # Catch-all to ensure all broken html is discarded
        return result


def clean_tweet(content):
    """
    Unescapes and replaces mentions and hashtags
    with static tokens (@ - MENTION, # - HASHTAG)
    """
    user_string = r'[A-Za-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff]'
    content = unescape(content)
    content = re.sub(r'([#|\uff03])(%s+)' % user_string,
                     lambda m: ' %s ' % m.group(2), content)
    content = re.sub(r'@%s{2,}' % user_string, ' MENTION ', content)
    content = re.sub(r'(RT(\x20)?(:)?)?', '', content)
    return content


def clean_reddit_comment(content):
    """
    Replace subreddit paths and user paths
    with static tokens (/r/... - SUBREDDIT)
    """
    content = unescape(content)
    content = re.sub(
        r'(\[deleted\])|(\[removed\])|(\[not found\])',
        '', content)
    content = re.sub(r'/?r/[0-9a-zA-Z_]{3,}', '', content)
    return content


def clean_general(content):
    """
    Remove any urls, non-ascii text and redundant spaces, normalize swearwords
    """
    # Urls
    content = re.sub(
        r'(http|https):/?/?[\w_-]*(?:\.[\w_-]*)?[\d\w.,@?^=%&:/~+#-]*',
        '', content)
    # Ascii
    content = re.sub(r'([^\x20-\x7f]*)*([\t\n\r]*)*', '', content)
    # Swearwords
    content = re.sub(
        r'[.,@?^=*%$\'";{}[\]<>|\\!&:/~+#-]{4,}',
        ' fucking ', content)
    # Spaces
    content = re.sub(r'\x20{2,}', ' ', content)
    return content


def clean_item(item):
    """
    Clean an item of undesirable data
    :param item: item to clean with all functions registered to item.source
    :return: cleaned item
    """
    content = item.content
    for func in _clean_functions[item.source]:
        content = func(content)
    return Item(content, topic=item.topic,
                source=item.source, created_at=item.created_at)


def classify_text(text, topic_query_dict):
    """
    Attempts to classify a text based on query strings organized by topic
    (Note, this is meant to be very fast - for a web spider)
    :param text: text to classify
    :param topic_query_dict: dict of topics and queries:
        e.g. {'t1': ['q1', 'q2'], 't2': ['q3'], ...
    :return: which topic does the text belong to
    """
    count = Counter(re.split(r'[^\w]', text.lower()))
    topic = ''
    max_count = 0
    for t, queries in topic_query_dict.items():
        c = sum(count[q] for q in queries)
        if c > max_count:
            max_count = c
            topic = t
    return topic


def extract_urls(text):
    """
    Extract urls in a given text and return the urls
    :param text: text to extract urls from
    :return: set of urls
    """
    urls = set()
    try:
        result = lxml.html.fromstring(text)
        for e in result.xpath('//*[@href]'):
            if e.get('href') is not None:
                urls.add(e.get('href'))
    finally:
        return urls


def remove_urls(text, remove=set(' )({}[];:')):
    """
    Removes (without returning) all urls present in a text
    :param text: text to clean urls from
    :param remove: break characters for url
    :return: text clean of urls
    """
    ind = text.find('http')
    while ind > -1:
        length = len(text)
        for i in range(ind + 7, length):
            if text[i] in remove:
                break
        else:
            i = length - 1
        text = text[:ind] + text[i:]
        ind = text.find('http')
    return text


register('twitter', clean_tweet, clean_general)
register('reddit', clean_reddit_comment, clean_general)
register('article', clean_article, clean_general)
register('blog', clean_article, clean_general)
register('spider', clean_article, clean_general)

__all__ = [
    'clean_article', 'clean_tweet', 'clean_reddit_comment', 'clean_general',
    'clean_item', 'register', 'unregister',
    'classify_text', 'extract_urls', 'remove_urls'
]
