from io import StringIO
import pandas as pd
from fin_database.steps.step import Step


class Parser(Step):

    def daily_process(self, input_, utils):
        lines = input_['data'].text.split('\n')
        stock_lines = []
        for l in lines:
            length = len(l.split('",'))
            if length > 16:
                stock_lines.append(l)

        stock_csv = "\n".join(stock_lines)
        stock_csv = stock_csv.replace('=', '')
        df = pd.read_csv(StringIO(stock_csv))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
        # df = df.set_index('證券代號')
        df_temp = df['證券代號']
        df = df.apply(lambda s: pd.to_numeric(s, errors='coerce'))
        df['證券代號'] = df_temp
        # coerce means show NaN if failed  # 沒有交易量的股票顯示"--"，轉譯過就變成NaN
        remain_label = df.columns[df.isnull().sum() != len(df)]
        df = df[remain_label]  # clear up whole NaN columns
        df.insert(0, '日期', input_['date'])
        df = df.set_index('日期')
        input_['data'] = df
        return input_

    def month_process(self, input_, utils):
        pass

    def f_report_process(self):
        pass

    def futures_process(self):
        pass