import sqlite3
import os
import datetime
import pandas as pd


class Data:

    def __init__(self):
        db = os.path.join('../findata', 'findata.sqlite')
        self.conn = sqlite3.connect(db)
        c = self.conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        table_names = [a[0] for a in c]

        self.date = datetime.datetime.now().date()
        self.dates = {}
        self.cn2table = {}

        for table in table_names:
            c = self.conn.execute(f"PRAGMA table_info({table})")
            cn_list = [a[1] for a in c]
            # print(cn_list)
            for cname in cn_list:
                self.cn2table[cname] = table

            if 'update_date' in cn_list:
                s = (f"SELECT DISTINCT update_date FROM {table}")
                self.dates[table] = pd.read_sql(s, self.conn, parse_dates=['update_date'], index_col=['update_date']).sort_index()

    def get(self, cn_name, n):
        if cn_name not in self.cn2table or n == 0:
            print(f'Error to get {cn_name} in database')
            return pd.DataFrame()

        else:
            dates_df = self.dates[self.cn2table[cn_name]].loc[:self.date].iloc[-n:]
            # print(dates_df)

            try:
                start_date = dates_df.index[-1]
                end_date = dates_df.index[0]
                start_str = (start_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                end_str = end_date.strftime('%Y-%m-%d')

            except:
                print(" Warning, fail to retrieve data", cn_name)
                return pd.DataFrame()

            if self.cn2table[cn_name] == 'FUTURES':

                s = (f"""SELECT product, 法人, update_date, {cn_name} FROM {self.cn2table[cn_name]} 
                WHERE update_date BETWEEN '{end_str}' AND '{start_str}'""")
                take = pd.read_sql(s, self.conn, parse_dates=['update_date']).pivot(index='update_date', columns=('product', '法人'))[cn_name]

            elif self.cn2table[cn_name] in {'BALANCE', 'CASH_FLOW', 'INCOME', 'MONTH_REVENUE', 'DAILY'}:

                s = (f"""SELECT stockID, update_date, {cn_name} FROM {self.cn2table[cn_name]} 
                WHERE update_date BETWEEN '{end_str}' AND '{start_str}'""")
                take = pd.read_sql(s, self.conn, parse_dates=['update_date']).pivot(index='update_date', columns='stockID')[cn_name]

            # elif self.cn2table[cn_name] == 'LIGHT':
            else:
                s = (f"""SELECT update_date, {cn_name} FROM {self.cn2table[cn_name]} 
                WHERE update_date BETWEEN '{end_str}' AND '{start_str}'""")
                take = pd.read_sql(s, self.conn, parse_dates=['update_date']).set_index('update_date')

            return take


def main():
    data = Data()
    data.date = datetime.date(2024, 4, 9)
    print(data.get('成交筆數', 3))
    # print(data.get('交易多方口數', 3))
    # print(data.get('存貨', 2))



if __name__ == '__main__':
    main()



