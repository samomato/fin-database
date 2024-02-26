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
        year, month_ = input_['month'].split('-')
        month_ = int(month_)
        roc_year = int(year) - 1911
        if roc_year < 102:
            url_end = ''
            start_num = 1
        else:
            url_end = '_0'
            start_num = 2
        url = f'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month_}{url_end}.html'
        res = requests.get(url)
        res.encoding = 'big5'
        input_['data'] = res
        input_['start_num'] = start_num
        input_['roc_year'] = roc_year
        return input_


    def f_report_process(self):
        pass

    def futures_process(self):
        pass