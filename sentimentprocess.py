# Class to pre-process incoming text data and prepare for feeding into neural network
import os
import time
from collections import defaultdict
from functools import partial
from multiprocessing import Process
from multiprocessing.connection import Client, Listener

import numpy as np
import tensorflow as tf

from base import BASE_DIR, Item
from neural_network_models.sentiment.model import Model

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


class SentimentStream(Process):
    """Sentiment calculation thread, sends average sentiments per time period calculated for all incoming items"""
    def __init__(self, send_every=60, incoming_port=6001, outgoing_port=6002):
        super(SentimentStream, self).__init__()
        self.sess = tf.Session()
        self.model = Model(BASE_DIR)
        tf.train.Saver().restore(self.sess, os.path.join(BASE_DIR, 'lib', 'bin', 'sentiment', 'binary'))

        self.batch_size = 64
        self.send_every = send_every
        self.incoming_port, self.outgoing_port = incoming_port, outgoing_port

        self.types = ['reddit', 'twitter', 'article', 'blog']
        self.current_sentiments = defaultdict(partial(defaultdict, list))
        self.last_sentiments = defaultdict(partial(defaultdict, float))
        self.running = True

    def calculate_sentiments(self, items):
        """Runs the sentiment calculator neural network on batch of incoming texts"""
        features = np.array([item.content for item in items], dtype=np.int32)
        predictions = self.sess.run(self.model.predictions, feed_dict={self.model.input_x: features})
        sentiments = []
        for i, item in enumerate(items):
            new_item = Item(predictions[i][1], item.topic, item.source)
            sentiments.append(new_item)
        return sentiments

    def send_to_parent(self, outgoing):
        """Averages collected sentiments, sends averaged value to output queue and resets lists"""
        for q in self.current_sentiments:
            for t in self.types:
                avg = float(sum(self.current_sentiments[q][t])) / max(1, len(self.current_sentiments[q][t]))
                self.last_sentiments[q][t] = avg if avg != 0 else self.last_sentiments[q][t]
                outgoing.send(Item(self.last_sentiments[q][t], q, t))
                self.current_sentiments[q][t] = []

    def run(self):
        incoming = Client(('localhost', self.incoming_port), authkey=b'veryscrape')
        listener = Listener(('localhost', self.outgoing_port), authkey=b'veryscrape')
        outgoing = listener.accept()

        start_time = time.time()
        while self.running:
            start = time.time()
            items = []
            # Collect items to process but make sure to send at least once per second and no blocking
            while len(items) < self.batch_size and time.time() - start < 1 and incoming.poll():
                item = incoming.recv()
                items.append(item)
            sentiments = self.calculate_sentiments(items)
            for item in sentiments:
                self.current_sentiments[item.topic][item.source].append(item.content)
            # Send results to parent if time
            now = time.time()
            if now - start_time >= self.send_every:
                self.send_to_parent(outgoing)
                start_time = now
        self.sess.close()
