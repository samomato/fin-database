from datetime import date
from time import sleep
import sqlite3
from fin_database.utils import Utils
from fin_database.steps.precheck import PreCheck
from fin_database.steps.crawler import Crawler
from fin_database.steps.parser import Parser
from fin_database.steps.storer import Storer
from progressbar import progressbar


class Pipeline:
    def produce(self, date_start, date_end, dtype):
        utils = Utils()
        match dtype:
            case 'daily':
                result = PreCheck().daily_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for date_ in result['date_list']:
                        input_ = {'date': date_, 'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                        for step in steps:
                            if input_['keep_run'] == False:
                                break
                            input_ = step.daily_process(input_, utils)

                        sleep(5)
                    result['conn'].close()

            case 'month':
                result = PreCheck().month_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for month in result['month_list']:
                        input_ = {'month': month, 'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                        for step in steps:
                            if input_['keep_run'] == False:
                                break
                            input_ = step.month_process(input_, utils)

                        sleep(5)
                    result['conn'].close()

            case 'f_report':
                result = PreCheck().f_report_check(date_start, date_end, utils)
                if result['keep_run'] == True:
                    steps = [Crawler(), Parser(), Storer()]
                    for season in result['season_list']:
                        seed = self.f_report_seed_generator(season, utils)
                        seed = self.f_report_seed_not_exist(season, seed, result['c'])
                        for company in progressbar(seed):
                            input_ = {'season': season, 'company': company, 'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                            sleep(5)
                            for step in steps:
                                if input_['keep_run'] == False:
                                    break
                                input_ = step.f_report_process(input_, utils)
                        # company = '3592' # only for test
                        # input_ = {'season': season, 'company': company, 'conn': result['conn'], 'c': result['c'], 'keep_run': True}
                        # for step in steps:
                        #     if input_['keep_run'] == False:
                        #         break
                        #     input_ = step.f_report_process(input_, utils)

                    result['conn'].close()

            case 'futures':
                pass

    def f_report_seed_generator(self, season, utils):  # 要在加檢查資料夾已有財報，若有完整財報則跳至PARSER步驟
        year, season = season.split('-')
        month = year + '-' + str(int(season)*3)
        input_ = {'month': month, 'conn': 'na', 'c': 'na2', 'keep_run': True}
        input_ = Crawler().month_process(input_, utils)
        input_ = Parser().month_process(input_, utils)
        seed = input_['data']['公司代號'].tolist()
        return seed

    def f_report_seed_not_exist(self, season, seed, c):
        new_seed = []
        try:
            for company in seed:
                c.execute(f"SELECT 公司代號 FROM 'CASH_FLOW' WHERE 季別='{season}' AND 公司代號='{company}'")
                if c.fetchone() == None:
                    new_seed.append(company)
        except sqlite3.OperationalError:
            print('maybe the 1st time before creating DB')
            new_seed = seed

        return new_seed

