from datetime import date
from fin_database.utils import Utils
from fin_database.steps.precheck import PreCheck
from fin_database.steps.crawler import Crawler
from fin_database.steps.parser import Parser
from fin_database.steps.storer import Storer

date_start = date(2020, 2, 6)
date_end = date(2020, 2, 12)

def main():

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

        result['conn'].close()
            # + waiting time

    # steps = [PreCheck(), Crawler(), ]

    # for step in steps:
    #     if input_['keep_run'] == False:
    #         print('The data already in DB')
    #         break
    #     input_, utils = step.daily_process(input_, utils)

    # input_, utils = PreFlight().daily_process(input_, utils)
    # input_, utils = Crawler().daily_process(input_, utils)

    # print(input_['date_list'])

if __name__ == '__main__':
    main()
