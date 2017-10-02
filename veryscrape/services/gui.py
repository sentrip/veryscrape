import tkinter as tk

from veryscrape import load_query_dictionary


class StreamStatusPage(tk.Frame):
    def __init__(self, master, column_ind=None, source=None):
        super(StreamStatusPage, self).__init__(master)
        self.labels = []
        self.button = tk.Button(master, text=source or 'status', command=self.lift)
        self.button.grid(row=0, column=column_ind * 2 or 0, sticky=tk.NSEW, columnspan=2, rowspan=1)
        self.grid(row=1, column=0, sticky=tk.NSEW, columnspan=10, rowspan=11)

        self.bad_color, self.good_color = '#ff8080', '#9fff80'
        self.topics = list(sorted(load_query_dictionary('query_topics').keys()))
        topics = iter(self.topics)
        for i in range(11):
            self.rowconfigure(i, weight=1)
            for j in range(10):
                self.columnconfigure(j, weight=1)
                t = next(topics)
                l = tk.Label(self, bd=2, bg=self.bad_color, text=t, relief='solid', font='Helvetica 14')
                l.grid(row=i, column=j, sticky=tk.NSEW)
                l.id = t
                self.labels.append(l)

    def change_label_color(self, color, ids):
        for i, label in enumerate([j for j in self.labels if j.id in ids]):
            self.labels[i].config(bg=color)


class Main(tk.Tk):
    def __init__(self, *args, **kwargs):
        super(Main, self).__init__(*args, **kwargs)
        types = ['article', 'blog', 'reddit', 'twitter', 'stock']
        self.frames = [StreamStatusPage(self, *pair) for pair in enumerate(types)]
        self.grid(12, 10, 1, 1)
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=11)
        for i in range(len(self.frames) * 2):
            self.grid_columnconfigure(i, weight=1)

if __name__ == '__main__':

    root = Main()
    # from threading import Thread
    # ks = list(sorted(load_query_dictionary('query_topics').keys()))
    # def send():
    #     input()
    #     root.frames[0].change_label_color(root.frames[0].good_color, ks)
    # Thread(target=send).start()
    root.mainloop()
