# This file contains the model architecture for the sentiment analyser
# Main model is required to be under the class Model with the following attributes:
# input_x, predictions
import os

import numpy as np
import tensorflow as tf
import tensorflow.contrib as ctb
from gensim.models import Word2Vec

bdr = tf.nn.bidirectional_dynamic_rnn


class Model:
    def __init__(self, base_directory):
        # Input layer
        self.input_x = tf.placeholder(tf.int32, [None, 30, 30], name='x')
        self.input_y = tf.placeholder(tf.int32, [None, 2], name='y')

        with tf.variable_scope('Init'):
            # Weights
            weights = {'word_context': tf.get_variable(name='word_context_attention_vector',
                                                       shape=[50 * 2],
                                                       initializer=ctb.layers.xavier_initializer(), dtype=tf.float32),
                       'sentence_context': tf.get_variable(name='sentence_context_attention_vector',
                                                           shape=[50 * 2],
                                                           initializer=ctb.layers.xavier_initializer(), dtype=tf.float32)}
            # Word & sentence encoders, input lengths
            word_cell_fw = ctb.rnn.GRUCell(50)
            word_cell_bw = ctb.rnn.GRUCell(50)
            sentence_cell_fw = ctb.rnn.GRUCell(50)
            sentence_cell_bw = ctb.rnn.GRUCell(50)

        # Embeddings
        with tf.variable_scope("Embedding"), tf.device('/cpu:0'):
            m = Word2Vec.load(os.path.join(base_directory, 'lib', 'bin', 'word2vec', 'model'))
            emb_mat = np.zeros([len(m.wv.vocab) + 2, 200])
            emb_mat[0:2] = np.random.uniform(2, 200)
            for w in m.wv.vocab:
                emb_mat[m.wv.vocab[w].index + 2, :] = m.wv[w]
            del m
            embedding_matrix = tf.constant(emb_mat, dtype=tf.float32)
            embedded_input = tf.nn.embedding_lookup(embedding_matrix, self.input_x)

        # Word level
        with tf.variable_scope('Word'):
            # Word encoding
            with tf.variable_scope('encoder') as sc:
                word_level_inputs = tf.reshape(embedded_input, [-1,
                                                                30,
                                                                200])
                (fw_outputs, bw_outputs), _ = bdr(cell_fw=word_cell_fw, cell_bw=word_cell_bw,
                                                  inputs=word_level_inputs,
                                                  dtype=tf.float32, swap_memory=True, scope=sc)
                word_encoder_outputs = tf.concat((fw_outputs, fw_outputs), 2)

            # Word attention
            with tf.variable_scope('attention'):
                input_projection = ctb.layers.fully_connected(word_encoder_outputs, 50 * 2,
                                                              activation_fn=tf.tanh)
                attention_weights = tf.nn.softmax(tf.multiply(input_projection, weights['word_context']))
                weighted_projection = tf.multiply(word_encoder_outputs, attention_weights)
                word_level_outputs = tf.reduce_sum(weighted_projection, axis=1)

        # Sentence Level
        with tf.variable_scope('Sentence'):
            # Sentence encoding
            with tf.variable_scope('encoder') as sc:
                sent_level_inputs = tf.reshape(word_level_outputs, [-1,  # params['batch_size'],
                                                                    30,
                                                                    50 * 2])
                (fw_outputs, bw_outputs), _ = bdr(cell_fw=sentence_cell_fw, cell_bw=sentence_cell_bw,
                                                  inputs=sent_level_inputs,  # sequence_length=sentence_input_lengths,
                                                  dtype=tf.float32, swap_memory=True, scope=sc)
                sentence_encoder_outputs = tf.concat((fw_outputs, fw_outputs), 2)

            # Sentence attention
            with tf.variable_scope('attention'):
                input_projection = ctb.layers.fully_connected(sentence_encoder_outputs, 50 * 2,
                                                              activation_fn=tf.tanh)
                attention_weights = tf.nn.softmax(tf.multiply(input_projection, weights['sentence_context']))
                weighted_projection = tf.multiply(sentence_encoder_outputs, attention_weights)
                sentence_level_outputs = tf.reduce_sum(weighted_projection, axis=1)
        # Prediction
        with tf.variable_scope("Prediction"):
            self.predictions = ctb.layers.fully_connected(sentence_level_outputs, 2,
                                                          activation_fn=tf.nn.softmax)
