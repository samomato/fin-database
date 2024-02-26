from datetime import date
from time import sleep
from fin_database.utils import Utils
from fin_database.steps.precheck import PreCheck
from fin_database.steps.crawler import Crawler
from fin_database.steps.parser import Parser
from fin_database.steps.storer import Storer


class Pipeline:
    def produce(self, date_start, date_end):
        utils = Utils()
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