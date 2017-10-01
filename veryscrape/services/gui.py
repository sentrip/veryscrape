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
        for j in range(10):
            tk.Grid.rowconfigure(self, j, weight=1)
            for i in range(11):
                tk.Grid.rowconfigure(self, i, weight=1)
                t = next(topics)
                l = tk.Label(self, bd=2, bg=self.bad_color, width=8, height=1,
                             text=t, relief='solid', font='Helvetica 14')
                l.id = t
                l.grid(column=i, row=j, sticky=tk.NSEW)
                self.labels.append(l)

    def change_label_color(self, color, ids):
        for i, label in enumerate([j for j in self.labels if j.id in ids]):
            self.labels[i].config(bg=color)


class Main(tk.Tk):
    def __init__(self, *args, **kwargs):
        super(Main, self).__init__(*args, **kwargs)
        self.container = tk.Frame()
        self.container.pack(side='top', fill='both', expand=True)
        self.frames = []
        self.subframes = []
        page_names = ['article', 'blog', 'reddit', 'twitter', 'stock']
        for i, t in enumerate(page_names):
            self.subframes.append([])
            f = tk.Frame(self.container)
            tk.Button(self.container, text=t, command=lambda: (f.lift(), self.subframes[i][0].lift())).grid(row=0, column=i, sticky='nwe')
            f.grid(row=1, column=0, columnspan=len(page_names), sticky=tk.NSEW)
            self.frames.append(f)

            # Stream status subframe
            subframe1 = SentimentPage(f, source=t, width=1200, height=800)
            tk.Button(f, text='Stream status', command=subframe1.lift).grid(row=0, column=0, sticky='nwe')
            subframe1.grid(row=1, column=0, columnspan=3, sticky=tk.NSEW)
            self.subframes[i].append(subframe1)

            # MAKE
            subframe2 = tk.Frame(f, width=1200, height=800)
            tk.Button(f, text='Unused', command=subframe2.lift).grid(row=0, column=1, sticky='nwe')
            subframe2.grid(row=1, column=0, columnspan=3, sticky=tk.NSEW)
            self.subframes[i].append(subframe2)
            
            # MAKE
            subframe3 = tk.Frame(f, width=1200, height=800)
            tk.Button(f, text='Unused', command=subframe3.lift).grid(row=0, column=2, sticky='nwe')
            subframe3.grid(row=1, column=0, columnspan=3, sticky=tk.NSEW)
            self.subframes[i].append(subframe3)

        self.frames[0].lift()
        self.subframes[0][0].lift()


if __name__ == '__main__':
    m = Main()
    m.mainloop()
