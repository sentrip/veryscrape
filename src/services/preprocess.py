# Class to pre-process incoming text data and prepare for feeding into neural network
import os
import re
from collections import defaultdict
from html import unescape
from multiprocessing import Process
from multiprocessing.connection import Listener, Client

import numpy as np
from gensim.models import Word2Vec
from lxml import html
import lxml
from newspaper import Config, extractors, cleaners, outputformatters
from nltk.tokenize import sent_tokenize, wordpunct_tokenize

from src.base import BASE_DIR, Item


def load_vocab(base_directory):
    """Loads vocabulary from Word2Vec model in base directory"""
    vocab = defaultdict(int)
    model = Word2Vec.load(os.path.join(base_directory, 'lib', 'bin', 'word2vec', 'model'))
    for word in model.wv.vocab:
        # Add 2 to index: 0 - blank, 1 - unknown
        vocab[word] = model.wv.vocab[word].index + 2
    return vocab


def clean_tweet(item):
    """Unescapes and replaces mentions and hashtags with static tokens (@ - MENTION, # - HASHTAG)"""
    content = unescape(item.content)
    content = re.sub(r'(^|[^0-9&]+)([#\uff03]+)([A-Za-z0-9_\u00c0-\u00d6\u00d8-\u00f6\u00f8-\u00ff]*)([#\uff03]*)',
                          ' HASHTAG ', content)
    content = re.sub(r'(RT(\x20)?(:)?)?(?<=^|(?<=[^a-zA-Z0-9-_.]))*(@)+([A-Za-z0-9_]+[A-Za-z0-9_]+)', ' MENTION ', content)
    return Item(content, item.topic, item.source)


def clean_reddit_comment(item):
    """Replace subreddit paths and user paths with static tokens (/r/... - SUBREDDIT, /u/ MENTION) """
    content = re.sub(r'<.*?>', '', item.content)
    content = re.sub(r'((\[deleted])|(\[removed])|(\[not found]))?', '', content)
    return Item(content, item.topic, item.source)


def clean_article(item):
    """Converts html text into article text"""
    config = Config()
    config.language = 'en'
    config.keep_article_html = False
    extractor = extractors.ContentExtractor(config)
    clean_doc = cleaners.DocumentCleaner(config).clean(html.fromstring(item.content))
    top_node = extractor.post_cleanup(extractor.calculate_best_node(clean_doc))
    content, _ = outputformatters.OutputFormatter(config).get_formatted(top_node)
    return Item(content, item.topic, item.source)


def clean_general(item):
    """Remove any urls, non-ascii text and redundant spaces, normalize swearwords"""
    # Urls
    content = re.sub(r'(http|ftp|https)(://)([\w_-]+(?:\.[\w_-]+)*)?([\d\w.,@?^=%&:/~+#-]*)?', '', item.content)
    # Ascii
    content = re.sub(r'([^\x20-\x7f]*)*([\t\n\r]*)*', '', content)
    # Swearwords
    content = re.sub(r'[.,@?^=*%$\'";{}[\]<>|\\\!&:/~+#-]{4,}', ' fucking ', content)
    # Spaces
    content = re.sub(r'\x20{2,}', ' ', content)
    return Item(content, item.topic, item.source)


def feature_convert(item, vocab, document_length=30, sentence_length=30):
    """Convert text of an item vector of shape [document_length * sentence_length] with word ids in the vector"""
    # Split sentences
    sentences = sent_tokenize(item.content, language='english')
    # Generator of lists of word ids
    feature_generator = map(lambda x: [vocab[q] if vocab[q] else 1 for q in x], map(wordpunct_tokenize, sentences))
    # Initialize feature vector
    features = np.zeros([document_length, sentence_length], dtype=np.int32)
    for i, sentence in enumerate(feature_generator):
        for j, word in enumerate(sentence):
            if i < document_length and j < sentence_length:
                features[i][j] = word
    return Item(features, item.topic, item.source)


class PreProcessWorker(Process):
    """Sentiment calculation thread, sends average sentiments per time period calculated for all incoming items"""
    def __init__(self, incoming_port=6000, outgoing_port=6001):
        super(PreProcessWorker, self).__init__()
        self.incoming_port, self.outgoing_port = incoming_port, outgoing_port
        self.vocab = load_vocab(BASE_DIR)
        self.clean_functions = {'reddit': clean_reddit_comment, 'twitter': clean_tweet,
                                'blog': clean_article, 'article': clean_article}
        self.running = True

    def run(self):
        incoming = Client(('localhost', self.incoming_port), authkey=b'veryscrape')
        outgoing = Listener(('localhost', self.outgoing_port), authkey=b'veryscrape').accept()

        while self.running:
            item = incoming.recv()
            try:
                item = self.clean_functions[item.source](item)
                item = clean_general(item)
                item = feature_convert(item, self.vocab)
                outgoing.send(item)
            except (AttributeError, ValueError, lxml.etree.XMLSyntaxError):
                pass
            except Exception as e:
                print('PreProcess', repr(e))
