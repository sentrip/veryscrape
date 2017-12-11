import asyncio
import re
from xml.sax.saxutils import unescape

import lxml.etree as etree
import lxml.html as html
from newspaper import Config, extractors, cleaners, outputformatters

from request import Item


def clean_tweet(item):
    """Unescapes and replaces mentions and hashtags with static tokens (@ - MENTION, # - HASHTAG)"""
    user_string = r'[A-Za-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff]'
    content = unescape(item.content)
    content = re.sub(r'([#|\uff03])(%s+)' % user_string, lambda m: ' %s ' % m.group(2), content)
    content = re.sub(r'@%s{2,}' % user_string, ' MENTION ', content)
    content = re.sub(r'(RT(\x20)?(:)?)?', '', content)
    return Item(content, item.topic, item.source)


def clean_reddit_comment(item):
    """Replace subreddit paths and user paths with static tokens (/r/... - SUBREDDIT) """
    content = unescape(item.content)
    content = re.sub(r'(\[deleted\])|(\[removed\])|(\[not found\])', '', content)
    content = re.sub(r'/?r/[0-9a-zA-Z_]{3,}', '', content)
    return Item(content, item.topic, item.source)


def clean_article(item):
    """Converts html text into article text"""
    # Html to article conversion with newspaper setup
    config = Config()
    config.language = 'en'
    config.keep_article_html = False
    extractor = extractors.ContentExtractor(config)
    cleaner = cleaners.DocumentCleaner(config)
    formatter = outputformatters.OutputFormatter(config)
    try:
        clean_doc = cleaner.clean(html.fromstring(item.content))
        top_node = extractor.post_cleanup(extractor.calculate_best_node(clean_doc))
        content, _ = formatter.get_formatted(top_node)
        return Item(content, item.topic, item.source)
    except (etree.XMLSyntaxError, AttributeError):
        pass
    except Exception as e:
        print('PreProcess', repr(e))
    return Item('failed', '', '')


def clean_general(item):
    """Remove any urls, non-ascii text and redundant spaces, normalize swearwords"""
    # Urls
    content = re.sub(r'(http|https):/?/?[\w_-]*(?:\.[\w_-]*)?[\d\w.,@?^=%&:/~+#-]*', '', item.content)
    # Ascii
    content = re.sub(r'([^\x20-\x7f]*)*([\t\n\r]*)*', '', content)
    # Swearwords
    content = re.sub(r'[.,@?^=*%$\'";{}[\]<>|\\!&:/~+#-]{4,}', ' fucking ', content)
    # Spaces
    content = re.sub(r'\x20{2,}', ' ', content)
    return Item(content, item.topic, item.source)


clean_functions = {'reddit': clean_reddit_comment, 'twitter': clean_tweet,
                   'blog': clean_article, 'article': clean_article}


def clean_item(item):
    item = clean_functions[item.source](item)
    item = clean_general(item)
    return item


class PreProcessor:
    def __init__(self, input_queue, output_queue):
        super(PreProcessor, self).__init__()
        self.input = input_queue
        self.output = output_queue
        self.running = True

    def send_item(self, future):
        item = future.result()
        if item.topic and item.source:
            self.output.put_nowait(item)

    async def run(self, pool):
        loop = asyncio.get_event_loop()
        while self.running:
            item = await self.input.get()
            future = loop.run_in_executor(pool, clean_item, item)
            future.add_done_callback(self.send_item)
