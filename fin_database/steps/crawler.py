import requests
from fin_database.steps.step import Step


class Crawler(Step):

    def daily_process(self, input_, utils):
        date_ = input_['date'].replace('-', '')
        url = f'''https://www.twse.com.tw/rwd/zh/afterTrading/
        MI_INDEX?date={date_}&type=ALLBUT0999&response=csv&_=1700894396517'''
        res = requests.get(url)

        if res.text == '':
            print(f'No data for {input_["date"]}. It may be Taiwan Holiday.')
            input_['keep_run'] = False

        input_['data'] = res
        return input_

    def month_process(self, input_, utils):
        pass

    def f_report_process(self):
        pass

    def futures_process(self):
        pass