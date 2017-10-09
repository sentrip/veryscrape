# Class to pre-process incoming text data and prepare for feeding into neural network
import os
import time
from multiprocessing import Queue
from threading import Thread

import numpy as np
import tensorflow as tf
import tensorflow.contrib as ctb
from gensim.models import KeyedVectors

from veryscrape.api import Item

bdr = tf.nn.bidirectional_dynamic_rnn
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


class Model:
    def __init__(self):
        super(Model, self).__init__()
        # Input layer
        self.input_x = tf.placeholder(tf.int32, [None, 150], name='x')
        base = os.path.join(os.getcwd(), 'src/veryscrape' if 'src' not in os.getcwd() else '').replace('vstest', 'veryscrape')
        m = KeyedVectors.load(os.path.join(base, 'bin', 'word2vec', 'GoogleNews-250k'))
        emb_mat = tf.constant(m.syn0)
        del m

        # Weights
        weights = [tf.get_variable(name='attention_vector_{}'.format(i), shape=[100 * 2], dtype=tf.float32,
                                   initializer=ctb.layers.xavier_initializer()) for i in range(5)]
        # Word & sentence encoders, input lengths
        word_cell_fw = ctb.rnn.GRUCell(100)
        word_cell_bw = ctb.rnn.GRUCell(100)

        # Embeddings
        with tf.device('/cpu:0'):
            embedded_input = tf.nn.embedding_lookup(emb_mat, self.input_x)

        # Encodings
        (fw_outputs, bw_outputs), _ = bdr(cell_fw=word_cell_fw, cell_bw=word_cell_bw, inputs=embedded_input,
                                          dtype=tf.float32, swap_memory=True)
        word_encoder_outputs = tf.concat((fw_outputs, fw_outputs), 2)

        # Attention
        input_projection = ctb.layers.fully_connected(word_encoder_outputs, 100 * 2, activation_fn=tf.tanh)
        attention_weights = tf.multiply(input_projection, weights[0])
        for att_w in weights[1:]:
            attention_weights = tf.multiply(attention_weights, att_w)
        attention_weights = tf.nn.softmax(attention_weights)
        weighted_projection = tf.multiply(word_encoder_outputs, attention_weights)
        word_level_outputs = tf.reduce_sum(weighted_projection, axis=1)

        # Prediction
        with tf.variable_scope("Prediction"):
            self.predictions = ctb.layers.fully_connected(word_level_outputs, 2, activation_fn=tf.nn.softmax)


class Sentiment(Thread):
    """Sentiment calculation thread, sends average sentiments per time period calculated for all incoming items"""
    def __init__(self, input_queue, output_queue):
        super(Sentiment, self).__init__()
        self.batch_size = 256
        self.input = input_queue
        self.output = output_queue

    def calculate_sentiments_from_queue(self, sess, model):
        """Runs the sentiment calculator neural network on batch of incoming texts"""
        while True:
            items = self.input.get()
            features = np.array([item.content for item in items], dtype=np.int32)
            predictions = sess.run(model.predictions, feed_dict={model.input_x: features})
            for i, item in enumerate(items):
                new_item = Item(predictions[i][1], item.topic, item.source)
                self.output.put(new_item)

    def run(self):
        batch_queue = Queue()
        with tf.Session() as sess:
            base = os.path.join(os.getcwd(), 'src/veryscrape' if 'src' not in os.getcwd() else '').replace('vstest', 'veryscrape')
            pth = os.path.join(base, 'bin', 'sentiment', 'binary')
            model = Model()
            tf.train.Saver().restore(sess, pth)
            place_in_queue = time.time()
            Thread(target=self.calculate_sentiments_from_queue, args=(sess, model, batch_queue,)).start()

            items = []
            while True:
                if not self.input.empty():
                    item = self.input.get_nowait()
                    items.append(item)
                    if len(items) >= self.batch_size or (time.time() - place_in_queue >= 1 and len(items) > 0):
                        batch_queue.put(items)
                        place_in_queue = time.time()
                        items = []


# class SentimentAverage(Thread):
#     """Sentiment calculation thread, sends average sentiments per time period calculated for all incoming items"""
#     def __init__(self, input_queue, output_queue, send_every=60):
#         super(SentimentAverage, self).__init__()
#         self.batch_size = 64
#         self.send_every = send_every
#         self.input = input_queue
#         self.output = output_queue
#         self.types = ['reddit', 'twitter', 'article', 'blog']
#         self.topics = sorted(list(get_topics('topics').keys()))
#         self.current_sentiments = {t: {q: [] for q in self.types} for t in self.topics}
#         self.last_sentiments = {t: {q: 0. for q in self.types} for t in self.topics}
#
#         self.count = 0
#         self.items_per_second = 0
#
#     def send_averages(self):
#         """Sends average values of collected sentiments over last `self.send_every` seconds and puts in output queue"""
#         data = {}
#         for t in self.types:
#             data[t] = {}
#             for q in self.topics:
#                 avg = float(sum(self.current_sentiments[q][t])) / max(1, len(self.current_sentiments[q][t]))
#                 self.last_sentiments[q][t] = avg if avg != 0 else self.last_sentiments[q][t]
#                 data[t][q] = self.last_sentiments[q][t]
#                 self.current_sentiments[q][t] = []
#         self.output.put(data)
#
#     def reset_count(self):
#         """Resets count of items per second and prints result"""
#         self.items_per_second = self.count
#         self.count = 0
#         print(self.items_per_second)
#
#     def get_next(self):
#         """Gets next item from input queue and adds to current sentiments list"""
#         item = self.input.get()
#         self.current_sentiments[item.topic][item.source].append(item.content)
#         self.count += 1
#
#     def run(self):
#         start_time = item_timer = time.time()
#         while True:
#             self.get_next()
#
#             now = time.time()
#             if now - start_time >= self.send_every:
#                 self.send_averages()
#                 start_time = now
#
#             if time.time() - item_timer >= 1:
#                 self.reset_count()
#                 item_timer = time.time()
