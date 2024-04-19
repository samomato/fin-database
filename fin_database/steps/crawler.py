import requests
from time import sleep
import random
from fin_database.steps.step import Step
from fin_database.settings import random_headers


class Crawler(Step):

    def daily_process(self, input_, utils):
        date_ = input_['date'].replace('-', '')
        header = random.choice(random_headers)
        # print(header['User-Agent'])  # just for test
        url = f'''https://www.twse.com.tw/rwd/zh/afterTrading/
        MI_INDEX?date={date_}&type=ALLBUT0999&response=csv&_=1700894396517'''
        print(f"crawling {input_['date']} price data...")
        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, timeout=5, headers=header)
        except Exception as e:
            print(e)
            print(f"other exception for {input_['date']}, please check")
            input_['keep_run'] = False
            return input_

        if r.text == '':
            print(f'No data for {input_["date"]}. It may be Taiwan Holiday.')
            input_['keep_run'] = False

        input_['data'] = r
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
        header = random.choice(random_headers)
        url = f'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month_}{url_end}.html'
        print(f"crawling {input_['month']} revenue data...")
        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, timeout=5, headers=header)
        except Exception as e:
            print(e)
            print(f"other exception for {input_['month']}, please check")
            input_['keep_run'] = False
            return input_

        r.encoding = 'big5'
        input_['data'] = r
        input_['start_num'] = start_num
        input_['roc_year'] = roc_year
        input_['roc_month'] = month_
        return input_

    def f_report_process(self, input_, utils):
        # 這裡確定資料夾是否已有載好的財報，有的話就讀出來return
        f_report_path = f'./f_report/{input_["season"]}/{input_["company"]}.html'
        if utils.check_file_exist(f_report_path):
            print(f"{input_['season']} {input_['company']} financial report had already download.")
            with open(f_report_path, 'r', encoding='UTF-8') as fr:
                r = fr.read()
                print(len(r))
            if len(r) < 4900:
                print('No any finance report')
                input_['keep_run'] = False
            else:
                input_['data'] = r
                input_['path'] = f_report_path

            return input_

        year, season = input_['season'].split('-')
        url = f'''https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={input_["company"]}&SYEAR={year}
            &SSEASON={season}&REPORT_ID'''
        header = random.choice(random_headers)
        print(f"crawling {input_['season']} {input_['company']} financial report data...")
        sleep(5)

        try:
            r = requests.get(url+'=C', timeout=5, headers=header)
            print(len(r.text))
            if len(r.text) == 564:
                print('查詢過於頻繁！！')
                sleep(30)
                header = random.choice(random_headers)
                r = requests.get(url + '=C', timeout=5, headers=header)

            elif 4900 > len(r.text) > 4500:
                print('Need 下載案例文件')
                input_['keep_run'] = False
                return input_

        except requests.exceptions.Timeout as err:
            print(err)
            sleep(30)
            r = requests.get(url+'=C')
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(30)
            r = requests.get(url+'=C', headers=header)
        except Exception as e:
            print(e)
            print(f"other exception for {input_['season']} {input_['company']}, please check")
            input_['keep_run'] = False
            return input_

        if len(r.text) <= 4500:
            sleep(5)
            print(f'{input_["company"]} in {input_["season"]}無合併財報')
            print(len(r.text))
            try:
                r = requests.get(url+'=A', timeout=5, headers=header)
            except requests.exceptions.Timeout as err:
                print(err)
                sleep(30)
                r = requests.get(url + '=A', headers=header)
            except requests.exceptions.ConnectionError:
                print('ConnectionError')
                print('try to wait 1 minute and retry...')
                sleep(30)
                r = requests.get(url + '=A', headers=header)
            except Exception as e:
                print(e)
                print(f"other exception for {input_['season']} {input_['company']}, please check")
                input_['keep_run'] = False
                return input_

            if len(r.text) <= 4500:
                sleep(5)
                print(f'{input_["company"]} in {input_["season"]}也無個別財報')
                try:
                    r = requests.get(url + '=B', timeout=5, headers=header)
                except requests.exceptions.Timeout as err:
                    print(err)
                    sleep(30)
                    r = requests.get(url + '=B', headers=header)
                except requests.exceptions.ConnectionError:
                    print('ConnectionError')
                    print('try to wait 1 minute and retry...')
                    sleep(30)
                    r = requests.get(url + '=B', headers=header)
                except Exception as e:
                    print(e)
                    print(f"other exception for {input_['season']} {input_['company']}, please check")
                    input_['keep_run'] = False
                    return input_

                if len(r.text) <= 4500:
                    print('No any finance report')
                    input_['keep_run'] = False

        r.encoding = 'big5'
        with open(f_report_path, 'w', encoding='UTF-8') as fr:
            fr.write(r.text)
        input_['data'] = r
        input_['path'] = f_report_path
        # input_['keep_run'] = False  # just for test
        return input_

    def futures_process(self, input_, utils):
        year, month, day = input_['date'].split('-')
        header = random.choice(random_headers)
        url = (f'https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&doQuery=1&queryDate'
               f'={year}%2F{month}%2F{day}')
        print(f"crawling {input_['date']} futures data...")
        sleep(8)
        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            header = random.choice(random_headers)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)

        if r.text == '':
            print(f'No data for {input_["date"]}. It may be Taiwan Holiday.')
            input_['keep_run'] = False

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['date'], 'futures request fail')

        return input_

    def taiex_process(self, input_, utils):
        year, month, day = str(input_['month']).split('-')
        header = random.choice(random_headers)
        url = (f'https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date={year + month}01&response=csv&_=1711551721071')
        print(f"crawling {input_['month']} taiex data...")

        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            header = random.choice(random_headers)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)
        if r.text == '':
            print(f'No data for {input_["month"]}. It may be the future.')
            input_['keep_run'] = False

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['month'], 'futures request fail')

        return input_

    def taiex_tr_process(self, input_, utils):
        year, month, day = str(input_['month']).split('-')
        header = random.choice(random_headers)
        url = (f'https://www.twse.com.tw/rwd/zh/TAIEX/MFI94U?date={year + month}01&response=csv&_=1712216964306')
        print(f"crawling {input_['month']} taiex-return data...")

        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            header = random.choice(random_headers)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)
        if r.text == '':
            print(f'No data for {input_["month"]}. It may be the future.')
            input_['keep_run'] = False

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['month'], 'futures request fail')

        return input_

    def tw50i_process(self, input_, utils):
        year, month, day = str(input_['month']).split('-')
        header = random.choice(random_headers)
        url = (f'https://www.twse.com.tw/rwd/zh/FTSE/TAI50I?date={year + month}01&response=csv&_=1712163756653')
        print(f"crawling {input_['month']} tw50 INDEX data...")

        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            header = random.choice(random_headers)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)
        if r.text == '':
            print(f'No data for {input_["month"]}. It may be the future.')
            input_['keep_run'] = False

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['month'], 'futures request fail')

        return input_

    def tw100i_process(self, input_, utils):
        year, month, day = str(input_['month']).split('-')
        header = random.choice(random_headers)
        url = (f'https://www.twse.com.tw/rwd/zh/FTSE/TAI100I?date={year + month}01&response=csv&_=1712163756653')
        print(f"crawling {input_['month']} tw100 INDEX data...")

        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            header = random.choice(random_headers)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)
        if r.text == '':
            print(f'No data for {input_["month"]}. It may be the future.')
            input_['keep_run'] = False

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['month'], 'futures request fail')

        return input_

    def sp500tr_process(self, input_, utils):
        start_year = input_['date_start'].year
        start_month = input_['date_start'].strftime('%b')
        start_date = input_['date_start'].strftime('%d')
        end_year = input_['date_end'].year
        end_month = input_['date_end'].strftime('%b')
        end_date = input_['date_end'].strftime('%d')
        header = random.choice(random_headers)
        url = (f'https://seekingalpha.com/api/v3/historical_prices?filter[ticker][slug]=sp500tr'
               f'&&filter[as_of_date][gte]=%20{start_month}%20{start_date}%20{start_year}'
               f'&filter[as_of_date][lte]=%20{end_month}%20{end_date}%20{end_year}&sort=as_of_date')

        header['authority'] = 'seekingalpha.com'
        header['accept-language'] = 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'
        header['accept'] = '*/*'
        header['cache-control'] = 'no-cache'
        header['Accept-Encoding'] = 'gzip, deflate, br, zstd'
        header['Referer'] = random.choice(['www.google.com', 'https://tw.stock.yahoo.com/markets', 'https://seekingalpha.com/'])
        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(input_['date_start'], ' to ', input_['date_end'], 'sp500tr request fail')

        return input_

    def vti_process(self, input_, utils):
        start_year = input_['date_start'].year
        start_month = input_['date_start'].strftime('%b')
        start_date = input_['date_start'].strftime('%d')
        end_year = input_['date_end'].year
        end_month = input_['date_end'].strftime('%b')
        end_date = input_['date_end'].strftime('%d')
        header = random.choice(random_headers)
        url = (f'https://seekingalpha.com/api/v3/historical_prices?filter[ticker][slug]=vti'
               f'&&filter[as_of_date][gte]=%20{start_month}%20{start_date}%20{start_year}'
               f'&filter[as_of_date][lte]=%20{end_month}%20{end_date}%20{end_year}&sort=as_of_date')

        header['authority'] = 'seekingalpha.com'
        header['accept-language'] = 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'
        header['accept'] = '*/*'
        header['cache-control'] = 'no-cache'
        header['Accept-Encoding'] = 'gzip, deflate, br, zstd'
        header['Referer'] = random.choice(['www.google.com', 'https://tw.stock.yahoo.com/markets', 'https://seekingalpha.com/'])
        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(r.status_code)
            print(input_['date_start'], ' to ', input_['date_end'], 'vti request fail')

        return input_

    def vix_process(self, input_, utils):
        start_year = input_['date_start'].year
        start_month = input_['date_start'].strftime('%b')
        start_date = input_['date_start'].strftime('%d')
        end_year = input_['date_end'].year
        end_month = input_['date_end'].strftime('%b')
        end_date = input_['date_end'].strftime('%d')
        header = random.choice(random_headers)
        url = (f'https://seekingalpha.com/api/v3/historical_prices?filter[ticker][slug]=vix'
               f'&&filter[as_of_date][gte]=%20{start_month}%20{start_date}%20{start_year}'
               f'&filter[as_of_date][lte]=%20{end_month}%20{end_date}%20{end_year}&sort=as_of_date')

        header['authority'] = 'seekingalpha.com'
        header['accept-language'] = 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'
        header['accept'] = '*/*'
        header['cache-control'] = 'no-cache'
        header['Accept-Encoding'] = 'gzip, deflate, br, zstd'
        header['Referer'] = random.choice(['www.google.com', 'https://tw.stock.yahoo.com/markets', 'https://seekingalpha.com/'])
        try:
            r = requests.get(url, timeout=5, headers=header)
        except requests.exceptions.Timeout as err:
            print(err)
            sleep(60)
            r = requests.get(url, headers=header)
        except requests.exceptions.ConnectionError:
            print('ConnectionError')
            print('try to wait 1 minute and retry...')
            sleep(60)
            r = requests.get(url, headers=header)

        if r.status_code == requests.codes.ok:
            input_['data'] = r
        else:
            input_['keep_run'] = False
            print(r.status_code)
            print(input_['date_start'], ' to ', input_['date_end'], 'vix request fail')

        return input_