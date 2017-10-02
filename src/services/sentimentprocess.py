# Class to pre-process incoming text data and prepare for feeding into neural network
import os
import time
from multiprocessing import Process, Queue
from multiprocessing.connection import Client, Listener
from threading import Thread

import numpy as np
import tensorflow as tf

from src.base import BASE_DIR, Item, Producer
from src.extensions.sentiment_model import Model

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


class SentimentWorker(Process):
    """Sentiment calculation thread, sends average sentiments per time period calculated for all incoming items"""
    def __init__(self, queue, port=6001):
        super(SentimentWorker, self).__init__()
        self.batch_size = 256
        self.port = port
        self.queue = queue

    def calculate_sentiments_from_queue(self, sess, model, queue):
        """Runs the sentiment calculator neural network on batch of incoming texts"""
        while True:
            items = queue.get()
            features = np.array([item.content for item in items], dtype=np.int32)
            predictions = sess.run(model.predictions, feed_dict={model.input_x: features})
            sentiments = []
            for i, item in enumerate(items):
                new_item = Item(predictions[i][1], item.topic, item.source)
                sentiments.append(new_item)
            for item in sentiments:
                self.queue.put(item)

    def run(self):
        batch_queue = Queue()
        incoming = Client(('localhost', self.port), authkey=b'vs')
        with tf.Session() as sess:
            items = []
            model = Model(BASE_DIR)
            tf.train.Saver().restore(sess, os.path.join(BASE_DIR, 'lib', 'bin', 'sentiment', 'binary'))
            place_in_queue = time.time()
            Thread(target=self.calculate_sentiments_from_queue, args=(sess, model, batch_queue,)).start()
            while True:
                if incoming.poll():
                    item = incoming.recv()
                    items.append(item)
                    if len(items) >= self.batch_size or (time.time() - place_in_queue >= 1 and len(items) > 0):
                        batch_queue.put(items)
                        place_in_queue = time.time()
                        items = []


class SentimentAverage(Thread):
    """Sentiment calculation thread, sends average sentiments per time period calculated for all incoming items"""
    def __init__(self, input_queue, send_every=60, port=6002):
        super(SentimentAverage, self).__init__()
        self.batch_size = 256
        self.send_every = send_every
        self.queue = input_queue
        self.port = port
        self.types = ['reddit', 'twitter', 'article', 'blog']
        self.topics = sorted(list(Producer.load_query_dictionary('query_topics.txt')))
        self.current_sentiments = {t: {q: [] for q in self.types} for t in self.topics}
        self.last_sentiments = {t: {q: -1 for q in self.types} for t in self.topics}

        self.count = 0
        self.items_per_second = 0

    def send_to_parent(self, outgoing):
        """Averages collected sentiments, sends averaged value to output queue and resets lists"""
        for q in self.topics:
            for t in self.types:
                avg = float(sum(self.current_sentiments[q][t])) / max(1, len(self.current_sentiments[q][t]))
                self.last_sentiments[q][t] = avg if avg != 0 else self.last_sentiments[q][t]
                outgoing.send(Item(self.last_sentiments[q][t], q, t))
                self.current_sentiments[q][t] = []

    def run(self):
        outgoing = Listener(('localhost', self.port), authkey=b'vs').accept()
        start_time = item_timer = time.time()
        while True:
            item = self.queue.get()
            self.current_sentiments[item.topic][item.source].append(item.content)
            self.count += 1

            now = time.time()
            if now - start_time >= self.send_every:
                self.send_to_parent(outgoing)
                start_time = now

            if time.time() - item_timer >= 1:
                self.items_per_second = self.count
                self.count = 0
                item_timer = time.time()
                print(self.items_per_second)
