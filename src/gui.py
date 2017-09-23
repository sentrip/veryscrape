import os
import time

import pandas as pd

BASE_DIR = "/home/djordje/Sentrip/"
if not os.path.isdir(BASE_DIR):
    BASE_DIR = "C:/users/djordje/desktop"


def get_n(df, t):
    return '{:3d}'.format(sum(df[a][t].as_matrix()[-1] != 0 for a in df.columns.levels[0])).replace(' ', '0') + '    '


def display():
    with open(os.path.join(BASE_DIR, 'lib', 'data', 'companyData.csv'), 'r') as f:
        df = pd.read_csv(f, header=[0, 1], index_col=0)
    print('Article', 'Blog   ', 'Reddit ', 'Twitter: ')
    print(get_n(df, 'article'), get_n(df, 'blog'), get_n(df, 'reddit'), get_n(df, 'twitter'))
    print()


while True:
    display()
    time.sleep(10)
