import math
import datetime
from datetime import date
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from data import Data
from test_strategy import test_strategy

warnings.simplefilter(action='ignore', category=FutureWarning)
# 加function 註解：
    # other_assets is a list

def backtest(start_date, end_date, holding_days, data, strategy, stop_loss=None, stop_profit=None, weight='cap',
             benchmark=None, total_asset=300,other_assets=None, asset_rate=None):

    # 先初始date物件日期來拿到所有結束日期前的個股收盤價
    data.date = end_date
    price = data.get('收盤價', (end_date - start_date).days)

    if other_assets is not None:
        if asset_rate is None:
            if len(other_assets) == 1:
                asset_rate = [0.8, 0.2]
            elif len(other_assets) == 2:
                asset_rate = [0.7, 0.15, 0.15]
            else:
                print('please set assets rate by yourself.')

        assets = []  # assets 裝除了選股策略外的所有資產收盤價
        for i in range(len(other_assets)):
            assets.append(data.get(other_assets[i], (end_date - start_date).days))

    # re-initialize the 1st trading date
    date_ = start_date
    # initialize some data for recording log later
    equality = pd.Series()
    nstock = {}  # 選出的個股數
    transections = pd.DataFrame()
    rratio = 1  # 初始 return ratio

    def fst_trading_day(date_):
        if date_ not in price.index:
            temp = price.loc[date_:]
            return temp.index[0]
        else:
            return date_

    # dates generator
    def dates_iter_periodicity(sdate, edate, hd):
        date_ = sdate
        while date_ < edate:
            yield date_, (date_ + datetime.timedelta(hd))
            date_ += datetime.timedelta(hd)

    # dates generator。給定一段date obj list，會再加上前後日期
    def dates_iter_assign(sdate, edate, hd):
        dlist = [sdate] + hd + [edate]
        if dlist[0] == dlist[1]:
            dlist = dlist[1:]
        if dlist[-1] == dlist[-2]:
            dlist = dlist[:-1]
        for s, e in zip(dlist[:-1], dlist[1:]):
            yield s, e

    # 獎generator 賦予到 dates
    if isinstance(holding_days, int):
        dates = dates_iter_periodicity(date_, end_date, holding_days)
    elif isinstance(holding_days, list):
        dates = dates_iter_assign(date_, end_date, holding_days)
    else:
        print('the type of holding_days should be list or int.')
        return None

    # 對 dates 夾娃娃。 這圈的ed會是下圈的sd。若直接用就是賣出日跟買進日同天
    for sd, ed in dates:
        data.date = sd  # get 會拿到含data.date當天日期 往前 n個值
        stocks = strategy(data)  # 返回index為個股代號，值為boolean的series
        print(sd, ed)
        # 以下s1為所選股票們在這段期間的收盤價，iloc[1:]表示跑策略(開始日期)的隔天收盤才買入(符合現況)
        s1 = price[[i for i in stocks.index if i in price.columns]][sd:ed].iloc[1:]  # n+1 到 ed

        if other_assets is not None:
            if len(other_assets) == 1:
                s2 = other_assets[0][sd:ed].iloc[1:]
            elif len(other_assets) == 2:
                s2 = other_assets[0][sd:ed].iloc[1:]
                s3 = other_assets[1][sd:ed].iloc[1:]
            elif len(other_assets) == 3:
                s2 = other_assets[0][sd:ed].iloc[1:]
                s3 = other_assets[1][sd:ed].iloc[1:]
                s4 = other_assets[2][sd:ed].iloc[1:]

        if s1.empty:  # 若沒選到，則製作比率為全為1的series，供待會使用
            s1 = pd.Series(1, index=pd.date_range(
                sd + datetime.timedelta(days=1), ed))
        else:
            if stop_loss != None:
                below_stop = ((s1 / s1.bfill().iloc[0]) - 1) * 100 < -np.abs(stop_loss)
                # 假設第 n+1 日收盤跌破停損點，則依第 n+2 日收盤價賣出做計算(作為sell)
                below_stop = (below_stop.cumsum() > 0).shift(2).fillna(False)
                # 把n+2後都變成NaN，指交易到n+2日
                s1[below_stop] = np.nan

            if stop_profit != None:
                above_stop = ((s1 / s1.bfill().iloc[0]) - 1) * 100 > np.abs(stop_profit)
                above_stop = (above_stop.cumsum() > 0).shift(2).fillna(False)
                s1[above_stop] = np.nan

            s1.dropna(axis=1, how='all', inplace=True)
            bprice = s1.bfill().iloc[0]
            # 把每一行後面的NaN都棄掉，再取最後一個元素(其實就是n+2的收盤價)
            sprice = s1.apply(lambda s: s.dropna().iloc[-1])
            transections = pd.concat([transections, pd.DataFrame({
                'buy_price': bprice,
                'sell_price': sprice,
                'lowest_price': s1.min(),
                'highest_price': s1.max(),
                'buy_date': pd.Series(s1.index[0], index=s1.columns),
                'sell_date': s1.apply(lambda s: s.dropna().index[-1]),
                'profit(%)': (sprice / bprice - 1) * 100
            })])

            s1.ffill(inplace=True)

            # normalize and average the price of each stocks
            s1 = s1 / s1.bfill().iloc[0]
            if weight == 'average':
                s1 = s1.mean(axis=1)
            elif weight == 'cap':

                股本 = (data.get('股本合計', 1))[stocks.index]
                price_ = (data.get('收盤價', 1))[stocks.index]
                cap = []
                for i in range(股本.size):
                    cap.append((股本.iloc[0, i]) * (price_.iloc[0, i]))
                    # (股本.iloc[1, i]) * (price_.iloc[1, i])
                    i += 1
                cap = [i/sum(cap) for i in cap]

                for i in range(s1.index.size):
                    cum = 0
                    for j in range(s1.columns.size):
                        cum += s1.iloc[i, j] * cap[j]
                        s1.iloc[i, 0] = cum

                s1 = s1.iloc[:, 0]
                print(s1)

            # s = s / s.bfill()[0]
        # print some log
        # print(sd, '-', ed,
        #       '報酬率: %.2f' % (s1.iloc[-1] / s1.iloc[0] * 100 - 100),
        #       '%', 'nstock', len(stocks))

        if other_assets is not None:
            # add other assets:
            s2 = s2 / s2.bfill().iloc[0]
            s2 = s2.mean(axis=1)
            s2 = s2.reindex(s1.index, method='ffill')
            print(s2)
            dratio1 = rratio * asset_rate[0]
            print(dratio1)
            dratio2 = rratio * asset_rate[1]
            print(dratio2)
            (((s1 * dratio1 + s2 * dratio2 - 1)) * 100).plot()
            rratio = s1[-1] / s1[0] * dratio1 + s2[-1] / s2[0] * dratio2
            print(rratio)
        else:
            ((s1 * rratio - 1) * 100).plot()
            equality = pd.concat([equality, s1 * rratio])
            rratio = (s1 / s1[0] * rratio).iloc[-1]

        if math.isnan(rratio):
            rratio = 1

        nstock[sd] = len(stocks)
        print('--------------------------next period----------------------------')

    if benchmark is None:
        benchmark = price['0050'][start_date:end_date].iloc[1:]

    ((benchmark / benchmark[0] - 1) * 100).plot(color=(0.8, 0.8, 0.8))
    plt.ylabel('Return On Investment (%)')
    plt.grid(linestyle='-.')
    plt.show()
    # ((benchmark/benchmark.cummax()-1)*100).plot(legend=True, color=(0.8, 0.8, 0.8))
    # ((equality/equality.cummax()-1)*100).plot(legend=True)
    # plt.ylabel('Dropdown (%)')
    # plt.grid(linestyle='-.')
    # plt.show()
    # pd.Series(nstock).plot.bar()
    # plt.ylabel('Number of stocks held')
    return equality, transections


if __name__ == '__main__':
    data = Data()
    # backtest(date(2022, 1, 1), date(2024, 2, 1), 60, data, test_strategy, other_asset='tlt', asset_rate=[0.9, 0.1])
    backtest(date(2022, 3, 1), date(2023, 12, 31), 90, data, test_strategy)
