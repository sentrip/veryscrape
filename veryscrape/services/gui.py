import tkinter as tk

from veryscrape import load_query_dictionary


class SentimentPage(tk.Frame):
    def __init__(self, *args, source=None, **kwargs):
        super(SentimentPage, self).__init__(*args, **kwargs)
        self.labels = []
        self.source = source
        self.bad_color, self.good_color = '#ff8080', '#9fff80'
        self.topics = list(sorted(load_query_dictionary('query_topics').keys()))
        topics = iter(self.topics)
        for j in range(11):
            for i in range(10):
                t = next(topics)
                l = tk.Label(self, bd=2, bg=self.bad_color,# width=8, height=1,
                             text=t, relief='solid', font='Helvetica 14')
                l.id = t
                l.grid(column=i, row=j, sticky=tk.N + tk.S + tk.E + tk.W)
                self.labels.append(l)

    def change_label_color(self, color, ids):
        for i, label in enumerate([j for j in self.labels if j.id in ids]):
            self.labels[i].config(bg=color)


class Main(tk.Tk):
    def __init__(self, *args, **kwargs):
        super(Main, self).__init__(*args, **kwargs)
        #self.container = tk.Frame(self)
        self.grid()#baseWidth=11, baseHeight=13, widthInc=1, heightInc=1)
        self.frames = []
        self.subframes = []
        page_names = ['article', 'blog', 'reddit', 'twitter', 'stock']
        for i, t in enumerate(page_names):
            #self.subframes.append([])
            f = SentimentPage(self, source=t)#, self.subframes[i][0].lift())).\
            tk.Button(self, text=t, command=lambda: (f.lift())).\
                grid(row=0, column=i*2, columnspan=2, sticky=tk.N + tk.S + tk.E + tk.W)
            f.grid(row=1, column=0, rowspan=11, columnspan=10, sticky=tk.N + tk.S + tk.E + tk.W)
            self.frames.append(f)
            #
            # # Stream status subframe
            # subframe1 = SentimentPage(f, source=t)
            # tk.Button(f, text='Stream status', command=subframe1.lift).grid(sticky=tk.N + tk.S + tk.E + tk.W, row=0)
            # subframe1.grid(sticky=tk.N + tk.S + tk.E + tk.W, row=1)
            # self.subframes[i].append(subframe1)

        self.frames[0].lift()
        # self.subframes[0][0].lift()


if __name__ == '__main__':
    m = Main()
    m.mainloop()
