import tkinter as tk
import random
from collections import defaultdict
from multiprocessing import Queue, Process
from multiprocessing.connection import Client, Listener
from threading import Thread
from src.base import Producer
import time
# good - #9fff80
# bad  - #ff8080
STICKY_ALL = tk.N + tk.E + tk.S + tk.W


class SentimentPage(tk.Frame):
    def __init__(self, *args, color=None, topics=None, source=None, **kwargs):
        super(SentimentPage, self).__init__(*args, **kwargs)
        self.color = color
        self.topics = topics or Producer.load_query_dictionary('query_topics.txt').keys()
        self.source = source or 'Stuff'
        self.labels = []
        topics = iter(self.topics)
        for j in range(10):
            tk.Grid.rowconfigure(self, j, weight=1)
            for i in range(11):
                tk.Grid.rowconfigure(self, i, weight=1)
                t = next(topics)
                l = tk.Label(self, bd=2, bg=self.color or 'black', width=10, height=3, text=t, relief='solid', font='Helvetica 16')
                l.id = t
                l.grid(column=i, row=j, sticky=STICKY_ALL)
                self.labels.append(l)

    def change_label_color(self, color, ids=None):
        if not ids:
            for i, label in enumerate(self.labels):
                self.labels[i].config(highlightcolor=color)
        else:
            for i, label in enumerate(self.labels):
                for _id in ids:
                    if 'id' in label.__dict__ and label.__dict__['id'] == _id:
                        self.labels[i].config(bg=color)


# def send():
#     topics = Producer.load_query_dictionary('query_topics.txt')
#     tps = ['article', 'blog', 'reddit', 'twitter', 'stock']
#     outgoing = Listener(('localhost', 6200), authkey=b'veryscrape').accept()
#     while True:
#         d = {}
#         for t in topics:
#             d[t] = {}
#             for k in tps:
#                 d[t][k] = -1 if random.random() < 0.5 else 1
#         outgoing.send(d)
#         print('sending')
#         time.sleep(5)


def distribute(queues):
    incoming = Client(('localhost', 6100), authkey=b'veryscrape')
    while True:
        i = incoming.recv()
        for q in queues.values():
            q.put(i)


def monitor(fr, queue):
    while True:
        items = queue.get()
        dd = {}
        for t in items:
            if items[t][fr.source.lower()] != -1:
                dd[t] = items[t][fr.source.lower()]
        fr.change_label_color('#ff8080', [t for t in items if t not in dd])
        fr.change_label_color('#9fff80', [t for t in items if t in dd])


def main():
    button_titles = ['Article', 'Blog', 'Reddit', 'Twitter', 'Stock']
    queues = {k: Queue() for k in button_titles}
    Thread(target=distribute, args=(queues,)).start()
    root = tk.Tk()
    topics = list(sorted(Producer.load_query_dictionary('query_topics.txt').keys()))
    main_frame = tk.Frame(root, width=1200, height=800)
    main_frame.grid(column=0, row=1, rowspan=11, columnspan=10,  sticky=STICKY_ALL)

    frames = [
        SentimentPage(main_frame, source='Article', color='#9fff80', topics=topics),
        SentimentPage(main_frame, source='Blog', color='#9fff80', topics=topics),
        SentimentPage(main_frame, source='Reddit', color='#9fff80', topics=topics),
        SentimentPage(main_frame, source='Twitter', color='#9fff80', topics=topics),
        SentimentPage(main_frame, source='Stock', color='#9fff80', topics=topics)
    ]

    for i, f in enumerate(frames):
        f.grid(column=0, row=1, rowspan=11, columnspan=10, sticky=STICKY_ALL)
        Thread(target=monitor, args=(frames[i], queues[frames[i].source],)).start()

    for i, n in enumerate(button_titles):
        _ = tk.Button(root, text=n, command=frames[i].lift).grid(column=i * 2, row=0, columnspan=2, sticky=STICKY_ALL)
    frames[0].lift()
    root.mainloop()


if __name__ == '__main__':
    #Thread(target=send).start()
    #time.sleep(1)
    main()









































































# # import os
# # import time
# #
# # import pandas as pd
# #
# # BASE_DIR = "/home/djordje/Sentrip/"
# # if not os.path.isdir(BASE_DIR):
# #     BASE_DIR = "C:/users/djordje/desktop"
# #
# #
# # def get_n(df, t):
# #     return '{:3d}'.format(sum(df[a][t].as_matrix()[-1] != 0 for a in df.columns.levels[0])).replace(' ', '0') + '    '
# #
# #
# # def display():
# #     with open(os.path.join(BASE_DIR, 'lib', 'data', 'companyData.csv'), 'r') as f:
# #         df = pd.read_csv(f, header=[0, 1], index_col=0)
# #     print('Article', 'Blog   ', 'Reddit ', 'Twitter', 'Stock  ')
# #     print(get_n(df, 'article'), get_n(df, 'blog'), get_n(df, 'reddit'), get_n(df, 'twitter'), get_n(df, 'stock'))
# #     print()
# #
# #
# # while True:
# #     display()
# #     time.sleep(15)
# # import tkinter as tk
# #
# #
# # class SentimentPage(tk.Frame):
# #     def __init__(self, button_gui, name, *args, **kwargs):
# #         tk.Frame.__init__(self, *args, **kwargs)
# #         b = tk.Button(button_gui[0], text=name, command=self.lift)
# #         b.grid(row=0, column=button_gui[2])
# #         self.place(in_=button_gui[1], x=0, y=0, relwidth=1, relheight=1)
# #         self.labels = []
# #         self.initialize_view()
# #
# #     def set_label_color(self, color):
# #         for label in self.labels:
# #             label.config(fg=color)
# #             self.update()
# #
# #     def initialize_view(self):
# #         for i in range(10):
# #             tk.Grid.rowconfigure(self, i, weight=1)
# #             for j in range(11):
# #                 tk.Grid.rowconfigure(self, j, weight=1)
# #                 kwargs = {'fg': 'black', 'text': '%d' % ((i + 1) * (j + 1))}
# #                 l = tk.Label(self, **kwargs)
# #                 l.grid(column=i, row=j, sticky=tk.N + tk.W + tk.S + tk.E)
# #                 self.labels.append(l)
# #
# #
# # class MainView(tk.Frame):
# #     def __init__(self, *args, **kwargs):
# #         tk.Frame.__init__(self, *args, **kwargs)
# #         tk.Grid.rowconfigure(args[0], 0, weight=1)
# #         tk.Grid.columnconfigure(args[0], 0, weight=1)
# #         buttonframe = tk.Frame(self)
# #         container = tk.Frame(self)
# #         buttonframe.grid(row=0, column=0, sticky=tk.W)
# #         container.grid(row=0, column=0, sticky=tk.W)
# #
# #         p1 = SentimentPage((buttonframe, container, 0), 'Twitter')
# #         p1.grid(column=0, row=1, sticky=tk.N + tk.W + tk.S + tk.E)
# #         p1.lift()
#
#
# # root = tk.Tk()
# # main = MainView(root)
# # main.grid(row=0, column=0)
# # root.wm_geometry("1200x800")
# # main.mainloop()
# # def change_red(labels):
# #     for l in labels:
# #         l.config(fg='red')
# #
# # labels = []
# # root = tk.Tk()
# # tk.Grid.rowconfigure(root, 0, weight=1)
# # tk.Grid.columnconfigure(root, 0, weight=1)
# #
# # frame = tk.Frame(root)
# # frame.grid(row=1, column=0, sticky=tk.N+tk.S+tk.E+tk.W)
# #
# # b = tk.Button(frame, command=lambda: print('button1'))
# # b.grid(row=0, column=0, columnspan=2, sticky=tk.N+tk.S+tk.E+tk.W)
# # b = tk.Button(frame, command=lambda: change_red(labels))
# # b.grid(row=0, column=2, columnspan=2, sticky=tk.N+tk.S+tk.E+tk.W)
# # b = tk.Button(frame, command=lambda: print('button3'))
# # b.grid(row=0, column=4, columnspan=2, sticky=tk.N+tk.S+tk.E+tk.W)
# # b = tk.Button(frame, command=lambda: print('button4'))
# # b.grid(row=0, column=6, columnspan=2, sticky=tk.N+tk.S+tk.E+tk.W)
# # b = tk.Button(frame, command=lambda: print('button5'))
# # b.grid(row=0, column=8, columnspan=2, sticky=tk.N+tk.S+tk.E+tk.W)
# #
# #
# # for row_index in range(1, 12):
# #     tk.Grid.rowconfigure(frame, row_index, weight=1)
# #     for col_index in range(10):
# #         tk.Grid.columnconfigure(frame, col_index, weight=1)
# #         lb = tk.Label(frame, text='test', width=10, height=5)
# #         lb.grid(row=row_index, column=col_index, sticky=tk.N+tk.S+tk.E+tk.W)
# #         labels.append(lb)
# #
# # root.mainloop()
# import tkinter as tk
# import random
#
#
# class SentimentPage:
#     def __init__(self, frame, name, index):
#         self.frame = frame
#         tk.Button(self, text=name, command=self.frame.lift).grid(row=0, column=index * 2, columnspan=2,
#                                                            sticky=tk.N+tk.S+tk.E+tk.W)
#         self.labels = []
#         self.initialize_view()
#
#     def initialize_view(self):
#         for row_index in range(1, 12):
#             tk.Grid.rowconfigure(self, row_index, weight=1)
#             for col_index in range(10):
#                 tk.Grid.columnconfigure(self, col_index, weight=1)
#                 lb = tk.Label(self, text='test')
#                 lb.id = 'test%d' % random.randint(1, 500)
#                 lb.grid(row=row_index, column=col_index, sticky=tk.N+tk.S+tk.E+tk.W)
#                 self.labels.append(lb)
#
#     def change_label_color(self, ids, color):
#         if not ids:
#             for label in self.labels:
#                 label.config(highlightcolor=color)
#         else:
#             for label in self.labels:
#                 for i in ids:
#                     if 'id' in label.__dict__ and label.__dict__['id'] == i:
#                         label.config(highlightcolor=color)
#
#
# labels = []
# root = tk.Tk()
# #tk.Grid.rowconfigure(root, 0, weight=1)
# #tk.Grid.columnconfigure(root, 0, weight=1)
#
# p1 = SentimentPage('Reddit', 0)
# p1.grid(row=0, column=0)
#
#
# p2 = SentimentPage('Twitter', 1)
# p2.grid(row=0, column=1)
#
# p3 = SentimentPage('Article', 2)
#
# p4 = SentimentPage('Blog', 3)
#
# p5 = SentimentPage('Stock', 4)
#
# p1.lift()
# root.mainloop()
