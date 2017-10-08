import asyncio
import os
import tkinter as tk
from collections import deque
from datetime import datetime
from functools import partial
from multiprocessing.connection import Listener
from threading import Thread

import aiohttp
import aiohttp.web as web
from sqlalchemy import create_engine, Table, MetaData, Column, Float, DateTime

RED, GREEN = '#ff8080', '#9fff80'


def load_query_dictionary(file_name):
    """Loads query topics and corresponding queries from disk"""
    queries = {}
    with open(os.path.join("/home/djordje/veryscrape/vs/documents", '%s.txt' % file_name), 'r') as f:
        lns = f.read().splitlines()
        for l in lns:
            x, y = l.split(':')
            queries[x] = y.split(',')
    return queries


class QueueWriter(Thread):
    def __init__(self, queue):
        super(QueueWriter, self).__init__()
        self.queue = queue
        self.db = create_engine('sqlite:///data/companyData.db')
        self.db.echo = False
        self.table_names = ['article', 'blog', 'twitter', 'reddit', 'stock']
        self.companies = sorted(list(load_query_dictionary('query_topics')))
        self.tables = {}
        for t in self.table_names:
            self.tables[t] = Table(t, MetaData(self.db),
                                   Column('time', DateTime, primary_key=True),
                                   *[Column(i, Float) for i in self.companies])
            if t not in self.db.table_names():
                self.tables[t].create()

    def update_database(self, data):
        current_time = datetime.now()
        for k in self.tables:
            i = self.tables[k].insert()
            data[k].update(time=current_time)
            i.execute(data[k])

    def run(self):
        while True:
            data = self.queue.get()
            self.update_database(data)


class StockGymEndPoint(Thread):
    def __init__(self, queue):
        super(StockGymEndPoint, self).__init__()
        self.queue = queue
        self.server = Listener(('localhost', 6100), authkey=b'veryscrape')
        self.conn = None
        self.max_items_in_queue = 10

    def accept_next(self):
        while True:
            try:
                self.conn = self.server.accept()
                break
            except:
                continue

    def run(self):
        self.accept_next()
        while True:
            if self.queue.qsize() > self.max_items_in_queue:
                allowed_items = deque(maxlen=self.max_items_in_queue)
                while not self.queue.empty():
                    allowed_items.append(self.queue.get_nowait())
                for item in allowed_items:
                    self.queue.put(item)
            else:
                data = self.queue.get()
                try:
                    self.conn.send(data)
                except:
                    self.accept_next()
                    self.conn.send(data)


class StreamStatusPage(tk.Frame):
    def __init__(self, master):
        super(StreamStatusPage, self).__init__(master)
        self.master = master
        self.companies = list(sorted(load_query_dictionary('query_topics').keys()))
        self.types = ['article', 'blog', 'reddit', 'twitter', 'stock']
        self.current_view = 'article'

        self.labels = {c: None for c in self.companies}
        self.statuses = {c: {k: RED for k in self.types} for c in self.companies}

        self.grid(row=1, column=0, sticky=tk.NSEW, columnspan=10, rowspan=11)
        self.reset()
        self.render('article')
        self.lift()

    def change_view(self, t):
        self.current_view = t
        self.render(t)

    def reset(self):
        for i, t in enumerate(self.types):
            button = tk.Button(self.master, text=t, command=partial(self.change_view, t))
            button.grid(row=0, column=i * 2, sticky=tk.NSEW, columnspan=2, rowspan=1)
        topics = iter(self.companies)
        for i in range(11):
            self.rowconfigure(i, weight=1)
            for j in range(10):
                self.columnconfigure(j, weight=1)
                c = next(topics)
                l = tk.Label(self, bd=2, bg=RED, text=c, relief='solid', font='Helvetica 14')
                l.grid(row=i, column=j, sticky=tk.NSEW)
                self.labels[c] = l

    def update_colors(self, t, color_dict):
        for k, c in color_dict.items():
            self.statuses[k][t] = c

    def render(self, t):
        for c in self.companies:
            self.labels[c].config(bg=self.statuses[c][t])
            self.labels[c].update()
        self.update()


class GUI(tk.Tk):
    def __init__(self, queue, *args, **kwargs):
        super(GUI, self).__init__(*args, **kwargs)
        grid_size = (12, 10, 1, 1)
        self.queue = queue
        self.protocol("WM_DELETE_WINDOW", self.end)
        self.status_frame = StreamStatusPage(self)
        self.grid(*grid_size)
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=11)
        for i in range(grid_size[1]):
            self.grid_columnconfigure(i, weight=1)

        self.running = True
        self.puller = Thread(target=self.pull)
        self.puller.start()

    def end(self):
        self.running = False
        self.puller.join()
        self.destroy()

    def pull(self):
        companies = list(sorted(load_query_dictionary('query_topics').keys()))
        if self.queue.qsize() >= 2:
            last = self.queue.get_nowait()
            while not self.queue.empty():
                last = self.queue.get_nowait()
            self.queue.put(last)
        while self.running:
            if not self.queue.empty():
                data = self.queue.get_nowait()
                for t, d in data.items():
                    default = {c: RED for c in companies}
                    for c, val in d.items():
                        if val > 0:
                            default[c] = GREEN
                    self.status_frame.update_colors(t, default)
                self.status_frame.render(self.status_frame.current_view)


class Controller(tk.Tk):
    def __init__(self, queue, *args, **kwargs):
        super(Controller, self).__init__(*args, **kwargs)
        self.queue = queue
        self.frame = tk.Frame(self, width=600, height=600)
        self.frame.pack(fill='both', expand=True)
        self.gui_button = tk.Button(self.frame, text='GUI', command=self.run_gui)
        self.gui_button.pack(side='top', fill='both', expand=True)

    def run_gui(self):
        GUI(self.queue).mainloop()


class MainServer(web.Server):
    def __init__(self, **kwargs):
        super(MainServer, self).__init__(self.process_request, **kwargs)
        self.queues = [asyncio.Queue()] * 3
        self.expected_keys = ['article', 'blog', 'reddit', 'twitter', 'stock']
        Thread(target=lambda: Controller(self.queues[0]).mainloop()).start()
        QueueWriter(self.queues[1]).start()
        StockGymEndPoint(self.queues[2]).start()

    async def process_request(self, request):
        try:
            if request.method != 'POST':
                raise TypeError
            data = await request.read()
            try:
                dct = eval(data)
                assert set(dct.keys()) == set(self.expected_keys)
                print(dct.copy().popitem()[1].popitem())
            except:
                raise TypeError
            for queue in self.queues:
                await queue.put(dct)
            return web.Response(text='Success!', status=200)

        except (TypeError, AssertionError):
            return web.Response(text="Incorrectly formatted request", status=404)


async def main_server(address):
    loop = asyncio.get_event_loop()
    session = aiohttp.ClientSession()
    server = MainServer()
    await loop.create_server(server, *address)
    while True:
        try:

            await asyncio.sleep(60)
        except KeyboardInterrupt:
            break

    session.close()
    await server.shutdown()
    loop.close()

if __name__ == '__main__':
    add = '192.168.1.53', 9999
    main_loop = asyncio.get_event_loop()
    main_loop.run_until_complete(main_server(add))
