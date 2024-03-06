import requests
from time import sleep
from fin_database.steps.step import Step

class Crawler(Step):

    def daily_process(self, input_, utils):
        date_ = input_['date'].replace('-', '')
        url = f'''https://www.twse.com.tw/rwd/zh/afterTrading/
        MI_INDEX?date={date_}&type=ALLBUT0999&response=csv&_=1700894396517'''
        print(f"crawling {input_['date']} price data...")
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
        print(f"crawling {input_['month']} revenue data...")
        res = requests.get(url)
        res.encoding = 'big5'
        input_['data'] = res
        input_['start_num'] = start_num
        input_['roc_year'] = roc_year
        return input_


    def f_report_process(self, input_, utils):
        year, season = input_['season'].split('-')
        url = f'''https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={input_["company"]}&SYEAR={year}
            &SSEASON={season}&REPORT_ID'''
        print(f"crawling {input_['date']} {input_['company']} financial report data...")
        try:
            print(f'start to request {input_["season"]} {input_["company"]}...')
            res = requests.get(url+'=C', timeout=5)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            res = requests.get(url+'=C')
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            res = requests.get(url+'=C')
        except Exception as e:
            print(e)
            res = [0 for i in range(152)]
            print(f"other exception for {input_['season']} {input_['company']}, please check")
            input_['keep_run'] = False
            return input_

        if len(res.text) < 150:
            sleep(5)
            print(f'{input_["company"]} in {input_["season"]}無合併財報')
            try:
                res = requests.get(url+'=A', timeout=5)
            except requests.exceptions.Timeout as err:
                print(err)
                sleep(60)
                res = requests.get(url + '=A')
            except requests.exceptions.ConnectionError:
                print('ConnectionError')
                print('try to wait 1 minute and retry...')
                sleep(60)
                res = requests.get(url + '=A')
            except Exception as e:
                print(e)
                res = [0 for i in range(152)]
                print(f"other exception for {input_['season']} {input_['company']}, please check")
                input_['keep_run'] = False
                return input_

            if len(res.text) < 150:
                sleep(5)
                print(f'{input_["company"]} in {input_["season"]}也無個別財報')
                try:
                    res = requests.get(url + '=B', timeout=5)
                except requests.exceptions.Timeout as err:
                    print(err)
                    sleep(60)
                    res = requests.get(url + '=B')
                except requests.exceptions.ConnectionError:
                    print('ConnectionError')
                    print('try to wait 1 minute and retry...')
                    sleep(60)
                    res = requests.get(url + '=B')
                except Exception as e:
                    print(e)
                    print(f"other exception for {input_['season']} {input_['company']}, please check")
                    input_['keep_run'] = False
                    return input_

                if len(res.text) < 120:
                    print('No any finance report')
                    input_['keep_run'] = False

        res.encoding = 'big5'
        f_report_path = f'./f_report/{input_["season"]}/{input_["company"]}.html'
        with open(f_report_path, 'w', encoding='UTF-8') as fr:
            fr.write(res.text)
        input_['data'] = res
        input_['path'] = f_report_path
        # input_['keep_run'] = False  # just for test
        return input_

    def futures_process(self, input_, utils):
        year, month, day = input_['date'].split('-')
        url = (f'https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&doQuery=1&queryDate'
               f'={year}%2F{month}%2F{day}')
        print(f"crawling {input_['date']} futures data...")
        try:
            r = requests.get(url, timeout=5)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(30)
            r = requests.get(url)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 0.5 minute and retry...')
            sleep(30)
            r = requests.get(url)

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['date'], 'futures request fail')

        return input_



