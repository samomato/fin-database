from io import StringIO
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from fin_database.steps.step import Step
from fin_database.settings import balance_temp, income_temp, cash_temp


class Parser(Step):

    def daily_process(self, input_, utils):
        print(f'Parsing price of {input_["date"]}...')
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
        df = df.rename(columns={'證券代號': 'stockID'})
        # df['stockID'] = df['stockID'].astype(str)
        remain_label = df.columns[df.isnull().sum() != len(df)]
        df = df[remain_label]  # clear up whole NaN columns
        df.insert(0, 'update_date', datetime.strptime(input_['date'], "%Y-%m-%d"))
        df = df.set_index(['update_date', 'stockID'])
        input_['data'] = df
        return input_

    def month_process(self, input_, utils):
        print(f'Parsing revenue of {input_["month"]}...')
        dfs = pd.read_html(StringIO(input_['data'].text))
        new_df = []

        for i in range(input_['start_num'], len(dfs), 2):
            new_df.append(dfs[i])

        df = pd.concat(new_df)
        new_label = []

        for i in range(len(df.columns)):
            new_label.append(df.columns[i][1].replace(' ',''))

        df.columns = new_label
        if input_['roc_year'] < 102:
            df = df.loc[~(df['公司代號'] == '合計')]
        else:
            df = df.loc[~(df['公司代號'] == '合計')].drop('備註', axis=1)
        df = df.rename(columns={'公司代號': 'stockID'})
        df.insert(0, 'update_date', input_['update_date'])
        df.insert(1, '月份', input_['month'])
        df = df.set_index(['update_date', 'stockID'])
        input_['data'] = df
        return input_

    def f_report_process(self, input_, utils):
        print(f"Parsing financial report of {input_['season']} of {input_['company']}...")
        with open(input_['path'],'r', encoding='UTF-8') as fr:
            data = fr.read().replace("<br>",'\n')
        dfs = pd.read_html(StringIO(data))
        year, _ = input_['season'].split('-')
        pd.options.mode.copy_on_write = True

        if int(year) >= 2019:
            # balance----------------------------------------------------------------------
            df_balance = dfs[0].iloc[:,1:3].astype(str)
            utils.fr_remain_ch(df_balance)
            df_balance.columns = [0, 1]
            df_balance = utils.fr_fit_template(df_balance, balance_temp)
            utils.fr_bracket2neg(df_balance)
            df_balance = utils.fr2my_sql_format(df_balance, input_['company'], input_['season'], input_['update_date'])

            # # income----------------------------------------------------------------------
            df_income = dfs[1].iloc[:, 1:3].astype(str)
            utils.fr_remain_ch(df_income)
            df_income.columns = [0, 1]
            df_income = utils.fr_fit_template(df_income, income_temp)
            utils.fr_bracket2neg(df_income)
            df_income = utils.fr2my_sql_format(df_income, input_['company'], input_['season'], input_['update_date'])
            #
            # # cash-flow:---------------------------------------------------------------------
            df_cash_flows = dfs[2].iloc[:, 1:3].astype(str)
            utils.fr_remain_ch(df_cash_flows)
            df_cash_flows.columns = [0, 1]
            df_cash_flows = utils.fr_fit_template(df_cash_flows, cash_temp)
            utils.fr_bracket2neg(df_cash_flows)
            df_cash_flows = utils.fr2my_sql_format(df_cash_flows, input_['company'],
                                                   input_['season'], input_['update_date'])

        else:
            # balance----------------------------------------------------------------------
            df_balance = dfs[1].iloc[:,:2].astype(str)
            utils.fr_remain_ch(df_balance)
            df_balance.columns = [0, 1]
            df_balance = utils.fr_fit_template(df_balance, balance_temp)
            df_balance = utils.fr2my_sql_format(df_balance, input_['company'], input_['season'], input_['update_date'])

            # income----------------------------------------------------------------------
            df_income = dfs[2].iloc[:,:2].astype(str)
            utils.fr_remain_ch(df_income)
            df_income.columns = [0, 1]
            df_income = utils.fr_fit_template(df_income, income_temp)
            df_income = utils.fr2my_sql_format(df_income, input_['company'], input_['season'], input_['update_date'])

            # cash-flow:---------------------------------------------------------------------
            df_cash_flows = dfs[3].iloc[:,:2].astype(str)
            utils.fr_remain_ch(df_cash_flows)
            df_cash_flows.columns = [0, 1]
            df_cash_flows = utils.fr_fit_template(df_cash_flows, cash_temp)
            df_cash_flows = utils.fr2my_sql_format(df_cash_flows, input_['company'],
                                                   input_['season'], input_['update_date'])

        input_['data'] = [df_balance, df_income, df_cash_flows]

        return input_

    def futures_process(self, input_, utils):
        print(f'Parsing futures of {input_["date"]}...')
        soup = BeautifulSoup(input_['data'].text, 'html.parser')
        trs = soup.find_all('tr', class_='12bk')  # 還沒解決當網頁沒data時秀出except訊息
        data_date = []
        if trs == []:
            print(f"No data for {input_['date']}. It may be Taiwan Holiday.")
            input_['keep_run'] = False
            return input_
        rows = trs[3:]
        # print(rows)
        i = 0
        product = '臺股期貨'
        for row in rows:

            names = row.find_all('th')
            cells = [name.text.strip() for name in names]
            nums = row.find_all('td')
            if cells[0] == '期貨小計':
                break
            if len(cells) > 1:
                product = [cells[1]]
                cells = cells[1:] + [num.text.strip() for num in nums]
            else:
                cells = product + cells + [num.text.strip() for num in nums]

            converted = [int(cell.replace(',', '')) for cell in cells[2:]]
            data_row = cells[:2] + converted

            data_date.append(data_row)

        new_label = ['product', '法人', '交易多方口數', '交易多方金額', '交易空方口數', '交易空方金額', '交易淨額口數',
                     '交易淨額金額',
                     '未平倉多方口數', '未平倉多方金額', '未平倉空方口數', '未平倉空方金額', '未平倉淨額口數',
                     '未平倉淨額金額']
        df = pd.DataFrame(data_date)
        df.columns = new_label
        df = df.iloc[:24]  # test only
        df.insert(0, 'update_date', datetime.strptime(input_['date'], "%Y-%m-%d"))
        df = df.set_index(['update_date', 'product', '法人'])
        # input_['keep_run'] = False  # test only
        input_['data'] = df
        return input_

    def taiex_process(self, input_, utils):
        print(f'Parsing taiex of {input_["month"]}...')
        lines = input_['data'].text.split('\r\n')
        pre_df = []
        for line in lines:
            contents = line.split('",')
            new = [c.replace('"', '') for c in contents]
            pre_df.append(new[:-1])

        df = pd.DataFrame(pre_df[2:-1])
        df.columns = ['update_date', '開盤', '最高', '最低', '收盤']
        df.iloc[:, 1:] = df.iloc[:, 1:].apply(lambda s: s.str.replace(',', ''))
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda s: str(int(s.split('/')[0])+1911)+'-'+s.split('/')[1]+'-'+s.split('/')[2])
        df = df.set_index('update_date')
        input_['data'] = df
        return input_

    def tw50i_process(self, input_, utils):
        print(f'Parsing taiex of {input_["month"]}...')
        lines = input_['data'].text.split('\r\n')
        pre_df = []
        for line in lines:
            contents = line.split('",')
            new = [c.replace('"', '') for c in contents]
            pre_df.append(new[:-1])

        df = pd.DataFrame(pre_df[2:-1])