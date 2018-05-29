from xml.sax.saxutils import unescape
from newspaper import Config, extractors, cleaners, outputformatters

import re
import lxml.etree as etree
import lxml.html as html

from .items import Item


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


def clean_article(content):
    """Converts html text into article text"""
    # Html to article conversion with newspaper setup
    config = Config()
    config.language = 'en'
    config.keep_article_html = False
    extractor = extractors.ContentExtractor(config)
    cleaner = cleaners.DocumentCleaner(config)
    formatter = outputformatters.OutputFormatter(config)
    try:
        clean_doc = cleaner.clean(html.fromstring(content))
        top_node = extractor.post_cleanup(
            extractor.calculate_best_node(clean_doc))
        content, _ = formatter.get_formatted(top_node)
        return content
    # Catch-all to ensure ALL broken html is discarded
    except:  # noqa
        return ''


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


clean_functions = {
    'reddit': clean_reddit_comment,
    'twitter': clean_tweet,
    'blog': clean_article,
    'article': clean_article
}


def clean_item(item):
    content = clean_functions[item.source](item.content)
    content = clean_general(content)
    return Item(content, topic=item.topic,
                source=item.source, created_at=item.created_at)


def remove_links(text, remove=set(' )({}[];:')):
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

