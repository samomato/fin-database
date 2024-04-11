from data import Data
import datetime
from datetime import date
import numpy as np
import pandas as pd
import warnings
import math
import matplotlib.pyplot as plt
from test_strategy import test_strategy

warnings.simplefilter(action='ignore', category=FutureWarning)
def bacltest(start_date, end_date, holding_days, data, strategy, stop_loss=None, stop_profit=None, weight='average', benchmark=None,):
    data.date = end_date
    print((end_date-start_date).days)
    price = data.get('收盤價', (end_date-start_date).days)
    print(price)
    # re-initialize the 1st trading date
    date_ = start_date  # initialize date
    # initialize some data for recording log later
    equality = pd.Series()
    nstock = {}  # 選出的個股數
    transections = pd.DataFrame()
    assets = 100  # 假定100萬台幣
    rratio = 1  # 初始 return ratio

    def fst_trading_day(date_):
        if date_ not in price.index:
            temp = price.loc[date_:]
            return temp.index[0]
        else:
            return date_

    def dates_periodicity(start_date, end_date, holding_days):
        date_ = start_date
        while date_ < end_date:
            yield date_, (date_ + datetime.timedelta(holding_days))
            date_ += datetime.timedelta(holding_days)

    def dates_assign(start_date, end_date, holding_days):
        dlist = [start_date] + holding_days + [end_date]
        if dlist[0] == dlist[1]:
            dlist = dlist[1:]
        if dlist[-1] == dlist[-2]:
            dlist = dlist[:-1]
        for s, e in zip(dlist, dlist[1:]):
            yield s, e

    if isinstance(holding_days, int):
        dates = dates_periodicity(date_, end_date, holding_days)
    elif isinstance(holding_days, list):
        dates = dates_assign(date_, end_date, holding_days)
    else:
        print('the type of holding_days should be list or int.')
        return None

    for sd, ed in dates:
        data.date = sd  # 要拿到每次投資開始當下的時間
        stocks = strategy(data)
        print(sd, ed)
        # print(price[[i for i in stocks.index if i in price.columns]][sd:ed])
        s = price[[i for i in stocks.index if i in price.columns]][sd:ed].iloc[1:]
        print(s)
        print('\n')
        # print(price[stocks.index & price.columns][s:e])
        if s.empty:  # 若沒選到，則維持最series比率為 1
            s = pd.Series(1, index=pd.date_range(
                sd + datetime.timedelta(days=1), ed))
        else:
            if stop_loss != None:
                below_stop = (
                    (s / s.bfill().iloc[0]) - 1)*100 < -np.abs(stop_loss)
                # 假設第 n+1 日收盤跌破停損點，則依第 n+2 日收盤價賣出做計算(作為sell)
                below_stop = (below_stop.cumsum() > 0).shift(2).fillna(False)
                # 把n+2後都變成NaN，指交易到n+2日
                s[below_stop] = np.nan

            if stop_profit != None:
                above_stop = (
                    (s / s.bfill().iloc[0]) - 1)*100 > np.abs(stop_profit)
                above_stop = (above_stop.cumsum() > 0).shift(2).fillna(False)
                s[above_stop] = np.nan

            s.dropna(axis=1, how='all', inplace=True)

            bprice = s.bfill().iloc[0]
            # 把每一行後面的NaN都棄掉，再取最後一個元素(其實就是n+2的收盤價)
            sprice = s.apply(lambda s: s.dropna().iloc[-1])
            print(bprice, sprice)
            transections = pd.concat([transections, pd.DataFrame({
                'buy_price': bprice,
                'sell_price': sprice,
                'lowest_price': s.min(),
                'highest_price': s.max(),
                'buy_date': pd.Series(s.index[0], index=s.columns),
                'sell_date': s.apply(lambda s: s.dropna().index[-1]),
                'profit(%)': (sprice / bprice - 1) * 100
            })])

            s.ffill(inplace=True)
            print(s)

            # normalize and average the price of each stocks
            if weight == 'average':
                s = s/s.bfill().iloc[0]
            s = s.mean(axis=1)
            print(s)
            # s = s / s.bfill()[0]
            # print(s)

        # print some log
        print(sd, '-', ed,
              '報酬率: %.2f' % (s.iloc[-1] / s.iloc[0] * 100 - 100),
              '%', 'nstock', len(stocks))

        ((s * rratio - 1) * 100).plot()
        equality = pd.concat([equality, s * rratio])
        rratio = (s / s[0] * rratio).iloc[-1]

        if math.isnan(rratio):
            rratio = 1

        nstock[sd] = len(stocks)
        print('--------------------------next period----------------------------')

    if benchmark is None:
        benchmark = price['0050'][start_date:end_date].iloc[1:]

    ((benchmark/benchmark[0]-1)*100).plot(color=(0.8, 0.8, 0.8))
    plt.ylabel('Return On Investment (%)')
    plt.grid(linestyle='-.')
    plt.show()
    ((benchmark/benchmark.cummax()-1)*100).plot(legend=True, color=(0.8, 0.8, 0.8))
    ((equality/equality.cummax()-1)*100).plot(legend=True)
    plt.ylabel('Dropdown (%)')
    plt.grid(linestyle='-.')
    plt.show()
    pd.Series(nstock).plot.bar()
    plt.ylabel('Number of stocks held')
    return equality, transections


if __name__ == '__main__':
    data = Data()
    bacltest(date(2021, 12, 1), date(2024, 2, 1), 90, data, test_strategy)
