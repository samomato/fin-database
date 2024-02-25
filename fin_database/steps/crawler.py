import requests
from fin_database.steps.step import Step


class Crawler(Step):

    def daily_process(self, input_, utils):
        # for date in input_['date_list']:
        date_ = input_['date'].replace('-', '')
        url = f'''https://www.twse.com.tw/rwd/zh/afterTrading/
        MI_INDEX?date={date_}&type=ALLBUT0999&response=csv&_=1700894396517'''
        res = requests.get(url)

        if res.text == '':
            print(f'No data for {input_["date"]}. It may be Taiwan Holiday.')
            input_['keep_run'] = False

        input_['data'] = res
        return input_
            #     continue
        # lines = res.text.split('\n')
        # stock_lines = []
        # for l in lines:
        #     length = len(l.split('",'))
        #     if length > 16:
        #         stock_lines.append(l)
        #
        # stock_csv = "\n".join(stock_lines)
        # stock_csv = stock_csv.replace('=', '')
        # df = pd.read_csv(StringIO(stock_csv))
        # df = df.astype(str)
        # df = df.apply(lambda s: s.str.replace(',', ''))
        # # df = df.set_index('證券代號')
        # df_temp = df['證券代號']
        # df = df.apply(lambda s: pd.to_numeric(s, errors='coerce'))
        # df['證券代號'] = df_temp
        # # coerce means show NaN if failed  # 沒有交易量的股票顯示"--"，轉譯過就變成NaN
        # remain_label = df.columns[df.isnull().sum() != len(df)]
        # df = df[remain_label]  # clear up whole NaN columns
        # df.insert(0, '日期', date)
        # df = df.set_index('日期')
        #
        # cn_list = ['日期']
        # [cn_list.append(_) for _ in df.columns]
        # input_['c'].execute('PRAGMA TABLE_INFO(DAILY)')
        # columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        # if cn_list != columns_of_tables:
        #     for cn in cn_list:
        #         if cn not in columns_of_tables:
        #             input_['c'].execute(f"ALTER TABLE 'DAILY' ADD COLUMN {cn}")
        #             print(f'Add new column {cn} to DB from {date}')
        #     input_['conn'].commit()
        # df.to_sql('DAILY', input_['conn'], if_exists='append')
        #
        # input_['conn'].close()
        # return input_, utils

    def month_process(self, input_, utils):
        pass

    def f_report_process(self):
        pass

    def futures_process(self):
        pass