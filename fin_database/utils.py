from datetime import timedelta
from datetime import date
import os
import re


class Utils:
    def calculate_date_period(self, date_start, date_end):
        period = (date_end - date_start).days
        a_day = timedelta(days=1)

        date_list = [date_start.isoformat()]
        for i in range(period):
            date_start = date_start + a_day
            date_list.append(date_start.isoformat())
        return date_list

    def calculate_month_period(self, date_start, date_end):
        period = (date_end - date_start).days
        delta = timedelta(days=1)
        today = date.today()
        # month_list = []
        update_list =[]
        for _ in range(int(period)+1):
            if date_start.day == 10:
                update_list.append(date_start)
                # month_list.append((date_start - timedelta(days=30)).strftime('%Y-%m'))
            date_start = date_start + delta

        return update_list

    def calculate_season_period(self, date_start, date_end):
        # 如果輸入的日期涵蓋到財報截止日，則將該截止日對應的財報年度季度收進list
        # Q4 -> 隔年3/31 ; Q1 -> 5/15 ; Q2 -> 8/14 ; Q3 -> 11/14
        a_day = timedelta(days=1)
        period = (date_end + a_day - date_start).days
        season_list = []
        update_list = []
        for i in range(period):
            if date_start.strftime('%m%d') == '0331':
                update_list.append(date_start)
                season = str(int(date_start.strftime('%Y'))-1) + '-4'
                season_list.append(season)
            elif date_start.strftime('%m%d') == '0515':
                update_list.append(date_start)
                season = date_start.strftime('%Y') + '-1'
                season_list.append(season)
            elif date_start.strftime('%m%d') == '0814':
                update_list.append(date_start)
                season = date_start.strftime('%Y') + '-2'
                season_list.append(season)
            elif date_start.strftime('%m%d') == '1114':
                update_list.append(date_start)
                season = date_start.strftime('%Y') + '-3'
                season_list.append(season)

            date_start = date_start + a_day

        return season_list, update_list

    def fr_remain_ch(self, df):
        i = 0
        for s in df.iloc[:, 0]:
            df.iloc[i, 0] = re.sub(r'[^\u4e00-\u9fa5]', '', s)
            i += 1
        return

    def fr_fit_template(self, df, temp):
        temp_bl = []
        for s in df[0]:
            if s in temp:
                temp_bl.append(True)
            else:
                temp_bl.append(False)
        df = df.iloc[temp_bl]
        return df

    def fr_bracket2neg(self, df):
        i = 0
        for n in df.iloc[:, 1]:
            df.iloc[i, 1] = n.replace('(', '-').replace(')', '').replace(',','')
            i += 1
        return

    def fr2my_sql_format(self, df, company, season, date_):

        df = df.T
        df.columns = df.iloc[0]
        df = df.drop(0)
        df.insert(0, '季別', season)
        df.insert(0, 'stockID', company)
        df.insert(0, 'update_date', date_)
        df.set_index(['update_date', 'stockID'])

        return df

    @staticmethod
    def make_dir(path):
        if not os.path.isdir(path):
            os.mkdir(path)

    @staticmethod
    def check_file_exist(file):
        return os.path.isfile(file)