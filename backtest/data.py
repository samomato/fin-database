import sqlite3
import os


class Data:

    def __init__(self):
        db = os.path.join('../findata', 'findata.sqlite')
        self.conn = sqlite3.connect(db)
        c = self.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        table_names = [a[0] for a in c]

        self.cn2table = {}
        for t in table_names:
            c = self.conn.execute(f"PRAGMA table_info({t})")
            cn_list = [a[1] for a in c]
            for cname in cn_list:
                self.cn2table[cname] = t
        # print([a[0] for a in c])
        # print(list(c))


Data()