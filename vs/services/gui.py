import tkinter as tk
from functools import partial
from threading import Thread
from vs import load_query_dictionary

RED, GREEN = '#ff8080', '#9fff80'


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

