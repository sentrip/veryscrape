import os

import tensorflow as tf
import tensorflow.contrib as ctb
from gensim.models import KeyedVectors

bdr = tf.nn.bidirectional_dynamic_rnn


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
