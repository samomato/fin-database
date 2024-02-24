from datetime import date
from fin_database.utils import Utils
from fin_database.steps.preflight import PreFlight
from fin_database.steps.crawler import Crawler

date_start = date(2020, 2, 6)
date_end = date(2020, 2, 13)

def main():
    input_ = {
        'date_start': date_start,
        'date_end': date_end,
        'keep_run': True,
    }

    utils = Utils()
    steps = [PreFlight(), Crawler(),]
    for step in steps:
        if input_['keep_run'] == False:
            print('The data already in DB')
            break
        input_, utils = step.daily_process(input_, utils)

    # input_, utils = PreFlight().daily_process(input_, utils)
    # input_, utils = Crawler().daily_process(input_, utils)

    # print(input_['date_list'])

if __name__ == '__main__':
    main()
