import pandas as pd
import sqlite3
import os
import matplotlib.pyplot as plt
import talib
from talib import abstract
from data import Data

#  目的：
#  繪製出美股與台股的買進訊號：
#  構想資產配置

#  練習TA-LIB：
#  1. 繪製美股QQQ (均線圖、MACD) -> ok
#  2. 繪製台股0050 (均線圖、MACD) -> ok

#  疊加各類報酬曲線: QQQ, VTI, TW50, TW100, TLT, BND 思考再平衡收益 -> ok
#  疊加台灣50與2330與景氣燈號: TAIEX_TR -> ok


def read_yahoo_sql(table):
    db = os.path.join('../findata', 'findata.sqlite')
    conn = sqlite3.connect(db)
    df = pd.read_sql(f'select update_date, {table}Open, {table}High, {table}Low, {table}Close, '
                         f'{table}Volume from {table}', conn, index_col=['update_date'], parse_dates=['update_date'])
    cns = df.columns.tolist()
    df.rename(columns={cns[0]: 'open', cns[1]: 'high', cns[2]: 'low', cns[3]: 'close', cns[4]: 'volume'}, inplace=True)
    return df


def read_daily_sql(stockID):
    db = os.path.join('../findata', 'findata.sqlite')
    conn = sqlite3.connect(db)
    df = pd.read_sql(f'select update_date, 開盤價, 最高價, 最低價, 收盤價, '
                     f'成交金額 from DAILY where stockID="{stockID}"', conn, index_col=['update_date'], parse_dates=['update_date'])
    cns = df.columns.tolist()
    df.rename(columns={cns[0]: 'open', cns[1]: 'high', cns[2]: 'low', cns[3]: 'close', cns[4]: 'volume'}, inplace=True)
    return df

def read_adj_price(table):
    db = os.path.join('../findata', 'findata.sqlite')
    conn = sqlite3.connect(db)
    df = pd.read_sql(f'select update_date, {table}AdjClose from {table}', conn, index_col=['update_date'], parse_dates=['update_date'])
    return df

def read_light():
    db = os.path.join('../findata', 'findata.sqlite')
    conn = sqlite3.connect(db)
    df = pd.read_sql(f'select update_date, light_score from LIGHT', conn, index_col=['update_date'], parse_dates=['update_date'])
    return df
def indicators_plot(tb, sdate=-701, edate=-1, stockID='0050'):
    if tb == 'DAILY':
        df = read_daily_sql(stockID)[sdate:edate]
    else:
        df = read_yahoo_sql(tb)[sdate:edate]

    fig, ax = plt.subplots(3, 1)
    plt.rcParams["figure.figsize"] = (10, 7)
    # ma and closed price
    ma20 = abstract.SMA(df, 20)
    ma60 = abstract.SMA(df, 60)
    ma180 = abstract.SMA(df, 120)
    ax[0].set_title(f'{edate-sdate} days {stockID} price')
    ax[0].grid()
    ax[0].plot(df['close'], label='close')
    ax[0].plot(ma20, label='20ma')
    ax[0].plot(ma60, label='60ma')
    ax[0].plot(ma180, label='180ma')
    ax[0].legend()
    # MACD
    macd = abstract.MACD(df, fastperiod=12, slowperiod=26, signalperiod=9)
    ax[1].set_title('MACD')
    ax[1].bar(macd.index, macd['macdhist'], color='m',width=1.1)
    ax[1].plot(macd['macdsignal'], color='b')
    ax[1].plot(macd['macd'], color='r')
    ax[1].grid()
    # RSI
    rsi = abstract.RSI(df, 14)
    ax[2].set_title('RSI')
    ax[2].grid()
    ax[2].plot(rsi)


    # plt.grid(which='both', axis='both')
    plt.show()


def compare_products(sdate=-2001, edate=-1):
    df_qqq = read_adj_price('QQQ')[sdate:edate]
    df_tlt = read_adj_price('TLT')[sdate:edate]
    df_bnd = read_adj_price('BND')[sdate:edate]
    df_tw50i = read_adj_price('TW50I')[int(sdate*0.98):edate]
    df_tw100i = read_adj_price('TW100I')[int(sdate * 0.98):edate]
    df_twlight = read_light()[-216:]
    print(df_twlight)

    df_qqq = df_qqq / df_qqq.iloc[0]
    df_tlt = df_tlt / df_tlt.iloc[0]
    df_bnd = df_bnd / df_bnd.iloc[0]
    df_tw50i = df_tw50i / df_tw50i.iloc[0]
    df_tw100i = df_tw100i / df_tw100i.iloc[0]

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(df_qqq.index, df_qqq, label='QQQ')
    # plt.plot(df_tlt.index, df_tlt, label='TLT')
    # plt.plot(df_bnd.index, df_bnd, label='BND')
    ax1.plot(df_tw50i.index, df_tw50i, label='TW50I', color='r')
    ax1.plot(df_tw100i.index, df_tw100i, label='TW100I')
    ax2.plot(df_twlight.index, df_twlight, label='TW LIGHT', linewidth=0.2, marker='o', color='b')

    plt.title('products_compare')
    ax1.set_ylabel('ratio with 1st date')
    ax2.set_ylabel('light score')
    ax2.grid(which='both')
    ax1.legend()
    ax1.set_yscale('log')
    plt.show()

    # 畫最大下跌風險圖
    plt.plot(df_qqq.index, ((df_qqq/df_qqq.cummax()-1)*100), label='QQQ')
    # plt.plot(df_tlt.index, ((df_tlt/df_tlt.cummax()-1)*100), label='TLT')
    # plt.plot(df_bnd.index, ((df_bnd / df_bnd.cummax() - 1) * 100), label='BND')
    plt.plot(df_tw50i.index, ((df_tw50i / df_tw50i.cummax() - 1) * 100), label='TW50I')
    plt.plot(df_tw100i.index, ((df_tw100i / df_tw100i.cummax() - 1) * 100), label='TW100I')
    plt.title('drop from highest')
    plt.ylabel('Dropdown (%)')
    plt.legend()
    plt.grid(linestyle='-.')
    plt.show()

if __name__ == '__main__':
    # indicators_plot('QQQ')
    # indicators_plot('VTI')
    # indicators_plot('DAILY', sdate=-2101, edate=-1401, stockID='0050')
    compare_products(sdate=-4601, edate=-1)

