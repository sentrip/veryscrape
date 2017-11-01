import re
import time
from concurrent.futures import ProcessPoolExecutor
from xml.sax.saxutils import unescape

import lxml.etree as etree
import lxml.html as html
from newspaper import Config, extractors, cleaners, outputformatters
from nltk.corpus import stopwords

from api import Item


class PreProcessor:
    def __init__(self, input_queue, output_queue):
        super(PreProcessor, self).__init__()
        self.input = input_queue
        self.output = output_queue
        self.clean_functions = {'reddit': self.clean_reddit_comment, 'twitter': self.clean_tweet,
                                'blog': self.clean_article, 'article': self.clean_article}
        # Html to article conversion with newspaper setup
        config = Config()
        config.language = 'en'
        config.keep_article_html = False
        self.extractor = extractors.ContentExtractor(config)
        self.cleaner = cleaners.DocumentCleaner(config)
        self.formatter = outputformatters.OutputFormatter(config)
        self.stopwords = set(stopwords.words('english') + list('~!@#$%^&*()_+{}|":?><><`1234567890-=][;,./\'\\'))

    @staticmethod
    def clean_tweet(item):
        """Unescapes and replaces mentions and hashtags with static tokens (@ - MENTION, # - HASHTAG)"""
        user_string = r'[A-Za-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff]'
        content = unescape(item.content)
        content = re.sub(r'[#|\uff03]%s+' % user_string, ' HASHTAG ', content)
        content = re.sub(r'@%s{2,}' % user_string, ' MENTION ', content)
        content = re.sub(r'(RT(\x20)?(:)?)?', '', content)
        return Item(content, item.topic, item.source)

    @staticmethod
    def clean_reddit_comment(item):
        """Replace subreddit paths and user paths with static tokens (/r/... - SUBREDDIT) """
        content = unescape(item.content)
        content = re.sub(r'(\[deleted\])|(\[removed\])|(\[not found\])', '', content)
        content = re.sub(r'/?r/[0-9a-zA-Z_]{3,}', '', content)
        return Item(content, item.topic, item.source)

    def clean_article(self, item):
        """Converts html text into article text"""
        try:
            clean_doc = self.cleaner.clean(html.fromstring(item.content))
            top_node = self.extractor.post_cleanup(self.extractor.calculate_best_node(clean_doc))
            content, _ = self.formatter.get_formatted(top_node)
            return Item(content, item.topic, item.source)
        except (etree.XMLSyntaxError, AttributeError):
            pass
        except Exception as e:
            print('PreProcess', repr(e))
        return Item('failed', '', '')

    @staticmethod
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

    def clean_item(self, item):
        item = self.clean_functions[item.source](item)
        item = self.clean_general(item)
        return item

    def send_item(self, future):
        item = future.result()
        if not isinstance(item.content, str) and item.topic and item.source:
            self.output.put(item)

    def run(self, n_workers=2):
        pool = ProcessPoolExecutor(n_workers)
        while True:
            if not self.input.empty():
                item = self.input.get_nowait()
                future = pool.submit(self.clean_item, item)
                future.add_done_callback(self.send_item)
            time.sleep(0.001)
