# Class for writing streamed data to local database
import datetime
import os
import time
from collections import defaultdict
from functools import partial
from multiprocessing import Process, Queue
from multiprocessing.connection import Client, Listener
from threading import Thread

from base import BASE_DIR, load_query_dictionary


class WriteWorker(Process):
    def __init__(self, file_lock, send_every=60, sentiment_port=6002, stock_port=6003, gym_port=6100):
        super(WriteWorker, self).__init__()
        self.file_lock = file_lock
        self.sentiment_port = sentiment_port
        self.stock_port = stock_port
        self.gym_port = gym_port
        self.send_every = send_every
        self.current_times = [0.] * 6
        self.query_topics = load_query_dictionary(os.path.join(BASE_DIR, 'lib', 'documents', 'query_topics1.txt'))
        self.file_name = os.path.join(BASE_DIR, 'lib', 'data', 'companyData.csv')
        self.subgroup_types = ['stock', 'reddit', 'twitter', 'article', 'blog']
        self.current_data = {c: {} for c in self.query_topics}
        self.gym_queue = Queue()
        self.running = True

    @property
    def ready_to_save(self):
        """Returns whether current_data contains enough data for saving to disk"""
        return sum(len(i) for c, i in self.current_data.items()) == len(self.query_topics) * len(self.subgroup_types)

    def create_file(self):
        """Creates file with required data labels for parsing with pandas if one does not exist"""
        if not os.path.isfile(self.file_name):
            with open(self.file_name, 'w') as f:
                f.write('company_name,' + ','.join([','.join([c]*len(self.subgroup_types)) for c in self.query_topics]) + '\n')
                f.write('time,' + ','.join([','.join(self.subgroup_types)]*len(self.query_topics)) + '\n')

    def save_incoming(self, incoming):
        """Receives item from queue and writes item data into appropriate data set and saves if required"""
        while self.running:
            item = incoming.recv()
            self.current_data[item.topic][item.source] = item.content
            self.gym_queue.put(item)

            if self.ready_to_save:
                time_string = '{:4d}-{:2d}-{:2d}|{:2d}:{:2d}:{:2d}'.format(*self.current_times).replace(' ','0').replace('|', ' ')
                data_string = ','.join(','.join('{}'.format(self.current_data[c][sg]) for sg in self.subgroup_types) for c in sorted(self.query_topics.keys()))
                with self.file_lock:
                    with open(self.file_name, 'a') as f:
                        f.write('{},{}\n'.format(time_string, data_string))
                self.current_data = {c: {} for c in self.query_topics}
                print('Wrote to disk at {}'.format(time_string))

    def send_to_gym(self, gym_queue):
        """Consumes items from gym queue and sends complete dictionary of a single time snapshot to gym environment"""
        l = Listener(('localhost', self.gym_port), authkey=b'veryscrape')
        outgoing = l.accept()
        while self.running:
            send_dictionary = defaultdict(partial(defaultdict, dict))
            while sum(len(i) for c, i in send_dictionary.items()) < len(self.query_topics) * len(self.subgroup_types):
                item = gym_queue.get()
                send_dictionary[item.topic][item.source] = item.content
            outgoing.send(send_dictionary)

    def update_time(self):
        """Updates current time array to represent the current time block (time blocks are write_every long)"""
        while self.running:
            st = time.time()
            ctime = datetime.datetime.today()
            self.current_times = [ctime.year, ctime.month, ctime.day, ctime.hour, ctime.minute, ctime.second]
            time.sleep(self.send_every - (time.time() - st))

    def initialize_work(self):
        self.create_file()
        gym_queue = Queue()
        stock = Client(('localhost', self.stock_port), authkey=b'veryscrape')
        sentiment = Client(('localhost', self.sentiment_port), authkey=b'veryscrape')
        Thread(target=self.update_time).start()
        Thread(target=self.save_incoming, args=(stock, )).start()
        Thread(target=self.save_incoming, args=(sentiment, )).start()
        Thread(target=self.send_to_gym, args=(gym_queue,)).start()
