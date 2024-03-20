import sqlite3
from time import sleep
from progressbar import progressbar
from fin_database.utils import Utils
from fin_database.steps.precheck import PreCheck
from fin_database.steps.crawler import Crawler
from fin_database.steps.parser import Parser
from fin_database.steps.storer import Storer


class Pipeline:
    def produce(self, date_start, date_end, dtype):
        utils = Utils()
        match dtype:
            case 'daily':
                result = PreCheck().daily_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for date_ in progressbar(result['date_list'], redirect_stdout=True):
                        input_ = {'date': date_, 'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                        for step in steps:
                            if input_['keep_run'] == False:
                                break
                            input_ = step.daily_process(input_, utils)

                        sleep(8)
                    result['conn'].close()

            case 'month':
                result = PreCheck().month_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for month in result['month_list']:
                        input_ = {'update_date': month[0], 'month': month[1],
                                  'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                        for step in steps:
                            if input_['keep_run'] == False:
                                break
                            input_ = step.month_process(input_, utils)

                        sleep(10)
                    result['conn'].close()

            case 'f_report':
                result = PreCheck().f_report_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for season, date_ in zip(result['season_list'], result['update_list']):
                        seed = self.f_report_seed_generator(season, date_, utils)
                        seed = self.company_need_process(season, seed, result['c'])
                        # print(season, '\n', date_)
                        # seed = ['2330']  # for test only
                        for company in seed:

                            input_ = {'season': season, 'update_date': date_, 'company': company,
                                      'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                            for step in steps:
                                if input_['keep_run'] == False:
                                    break
                                input_ = step.f_report_process(input_, utils)

                    result['conn'].close()

            case 'futures':
                result = PreCheck().futures_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for date_ in result['date_list']:
                        input_ = {'date': date_, 'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                        for step in steps:
                            if input_['keep_run'] == False:
                                break
                            input_ = step.futures_process(input_, utils)

                        sleep(10)
                    result['conn'].close()

    @staticmethod
    def f_report_seed_generator(season, date_, utils):  # 要在加檢查資料夾已有財報，若有完整財報則跳至PARSER步驟
        year, season = season.split('-')
        month = year + '-' + str(int(season)*3)
        input_ = {'month': month, 'update_date': date_, 'conn': 'na', 'c': 'na2', 'keep_run': True}
        input_ = Crawler().month_process(input_, utils)
        input_ = Parser().month_process(input_, utils)
        seed = [tup[1] for tup in input_['data'].index]
        return seed

    @staticmethod
    def company_need_process(season, seed, c):
        new_seed = []
        try:
            for company in seed:
                c.execute(f"SELECT stockID FROM 'BALANCE' WHERE 季別='{season}' AND stockID='{company}'")
                f1 = c.fetchone()
                c.execute(f"SELECT stockID FROM 'CASH_FLOW' WHERE 季別='{season}' AND stockID='{company}'")
                f2 = c.fetchone()
                c.execute(f"SELECT stockID FROM 'INCOME' WHERE 季別='{season}' AND stockID='{company}'")
                f3 = c.fetchone()
                if (f1 and f2 and f3) is None:
                    new_seed.append(company)
        except sqlite3.OperationalError:
            print('maybe the 1st time before creating DB')
            new_seed = seed

        return new_seed

