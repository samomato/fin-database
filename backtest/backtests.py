import math
import datetime
from datetime import date
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from data import Data
from test_strategy import test_strategy
from top_small_strategy import top_small_strategy

warnings.simplefilter(action='ignore', category=FutureWarning)
# 加function 註解：


def backtest(start_date, end_date, holding_days, data, strategy, stop_loss=None, stop_profit=None, weight='average',
             benchmark=None):

    # 先初始date物件日期來拿到所有結束日期前的個股收盤價
    data.date = end_date
    price = data.get('收盤價', (end_date - start_date).days)

    # re-initialize the 1st trading date
    date_ = start_date
    # initialize some data for recording log later
    equality = pd.Series()
    transections = pd.DataFrame()
    nstock = {}  # 選出的個股數
    rratio = 1  # 初始 return ratio

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
            buy_price = s1.bfill().iloc[0]
            # 把每一行後面的NaN都棄掉，再取最後一個元素(其實就是n+2的收盤價)
            # sell_price = s1.apply(lambda s: s.dropna().iloc[-1])
            s1.ffill(inplace=True)
            sell_price = s1.iloc[-1]

            transections = pd.concat([transections, pd.DataFrame({
                'buy_price': buy_price,
                'sell_price': sell_price,
                'lowest_price': s1.min(),
                'highest_price': s1.max(),
                'buy_date': pd.Series(s1.index[0], index=s1.columns),
                'sell_date': s1.apply(lambda s: s.dropna().index[-1]),
                'profit(%)': (sell_price / buy_price - 1) * 100
            })])

            # normalize and average the price of each stocks
            s1 = s1.bfill() / s1.bfill().iloc[0]
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

        # print some log
        print(sd, '-', ed,
              '報酬率: %.2f' % (s1.iloc[-1] / s1.iloc[0] * 100 - 100),
              '%', 'nstock', len(stocks))
        # add nstock history
        nstock[sd] = len(stocks)

        plt.figure(1)
        ((s1 * rratio - 1) * 100).plot()
        # (s1 * rratio - 1).plot()
        # plt.yscale('symlog')
        equality = pd.concat([equality, s1 * rratio])
        rratio = (s1 / s1[0] * rratio).iloc[-1]

        if math.isnan(rratio):
            rratio = 1

        nstock[sd] = len(stocks)
        print('--------------------------next period----------------------------')

    if benchmark is None:
        benchmark = price['0050'][start_date:end_date].iloc[1:]

    ((benchmark / benchmark[0] - 1) * 100).plot(color=(0.8, 0.8, 0.8))
    # (benchmark / benchmark[0] - 1).plot(color=(0.8, 0.8, 0.8))
    # plt.yscale('symlog')
    # plt.ylabel('Return On Investment Ratio')
    plt.ylabel('Return On Investment (%)')
    plt.xlabel('time (year)')
    plt.grid(linestyle='-.')
    plt.title('TOP SMALL STRATEGY BACKTEST')

    plt.figure(2)
    ((benchmark/benchmark.cummax()-1)*100).plot(legend=True, label='ETF 0050', color=(0.8, 0.8, 0.8))
    ((equality/equality.cummax()-1)*100).plot(legend=True, label='my strategy', linewidth=1.5, color='g', linestyle='dotted')
    plt.ylabel('Dropdown (%)')
    plt.xlabel('time (year)')
    plt.title('DROP FROM HIGHEST')
    plt.grid(linestyle='-.')

    plt.figure(3)
    pd.Series(nstock).plot.bar()
    plt.ylabel('Number of stocks')
    plt.xlabel('time')
    plt.title('NUMBER OF SELECTED STOCKS')
    plt.show()

    return equality, transections


def hold_days(start_year, end_year):
    years = []
    for i in range(end_year - start_year + 1):
        years.append(start_year + i)
    months = [[3, 31], [5, 15], [8, 14], [11, 14]]
    hold_list = []
    for y in years:
        if y > datetime.date.today().year:
            break
        for m in months:
            if (y == datetime.date.today().year) and (m[0] >= datetime.date.today().month):
                break
            hold_list.append(datetime.date(y, m[0], m[1]))
    return hold_list


def hold_months(start_date, end_date, months=2):
    hold_days = [start_date]
    sy = start_date.year
    sm = start_date.month
    sd = start_date.day
    while (sy < date.today().year) or (sy == date.today().year and sm < date.today().month):
        sm += months
        if sm >= 13:
            sy += 1
            sm = sm-12
        if end_date > date(sy, sm, sd):
            hold_days.append(datetime.date(sy, sm, sd))
    return hold_days


if __name__ == '__main__':
    data = Data()
    hold_days = hold_months(date(2014, 7, 10), date(2024, 4, 10), 2)
    # hold_days = hold_60days()
    # backtest(date(2022, 3, 1), date(2023, 12, 31), 90, data, test_strategy)
    backtest(date(2014, 7, 10), date(2024, 4, 19), hold_days, data, top_small_strategy, weight='average')