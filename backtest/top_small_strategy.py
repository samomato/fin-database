import datetime
import pandas as pd
import matplotlib.pyplot as plt
from backtest.data import Data

data = Data()
data.date = datetime.date(2024,4,10)
# # 股本 share capital


def top_small_strategy(data):
    close = data.get('收盤價', 280)
# ------------------------------------------------------------------------------------------------------------------
    #  市值
    sc = data.get('股本合計', 1)
    cap = sc.iloc[0] * 1000 / 10 * close.reindex(sc.index, method='ffill').iloc[0]  # 市值
    if (data.date.year == 2016) and (data.date.month > 5):
        cap['6531'] = cap['6531'] * (10 / 5)
    elif data.date.year > 2016:
        cap['6531'] = cap['6531'] * (10 / 5)

# ------------------------------------------------------------------------------------------------------------------
    # 股價淨值比 Price to book ratio
    歸屬業主權益 = data.get('歸屬於母公司業主之權益合計', 1)
    權益總計 = data.get('權益總計', 1)
    權益總額 = data.get('權益總額', 1)
    sc = data.get('股本合計', 1)
    歸屬業主權益.fillna(權益總計, inplace=True)
    歸屬業主權益.fillna(權益總額, inplace=True)
    # pb_ratio = close.reindex(sc.index, method='ffill') / (歸屬業主權益 / sc) / 10
    pb_ratio = close.iloc[-1] / (歸屬業主權益 / sc) / 10  # 用最近的股價去算pb ratio 較不失真 比免股票這段期間暴漲
    if (data.date.year == 2016) and (data.date.month > 5):
        pb_ratio['6531'] = pb_ratio['6531']*2
    elif data.date.year > 2016:
        pb_ratio['6531'] = pb_ratio['6531']*2

    # 幫助有限
# ------------------------------------------------------------------------------------------------------------------
    # 自由現金流 net cash flow
    def toSeasonal(df):
        season4 = df[df.index.month == 3]
        season1 = df[df.index.month == 5]
        season2 = df[df.index.month == 8]
        season3 = df[df.index.month == 11]

        season1.index = season1.index.year
        season2.index = season2.index.year
        season3.index = season3.index.year
        season4.index = season4.index.year - 1

        newseason1 = season1
        newseason2 = season2 - season1.reindex_like(season2)
        newseason3 = season3 - season2.reindex_like(season3)
        newseason4 = season4 - season3.reindex_like(season4)

        newseason1.index = pd.to_datetime(newseason1.index.astype(str) + '-05-15')
        newseason2.index = pd.to_datetime(newseason2.index.astype(str) + '-08-14')
        newseason3.index = pd.to_datetime(newseason3.index.astype(str) + '-11-14')
        newseason4.index = pd.to_datetime((newseason4.index + 1).astype(str) + '-03-31')
        return pd.concat([newseason4, newseason1, newseason2, newseason3]).sort_index()

    投資現金流 = toSeasonal(data.get('投資活動之淨現金流入流出', 4))
    營業現金流 = toSeasonal(data.get('營業活動之淨現金流入流出', 4))
    net_flow = (投資現金流 + 營業現金流).iloc[-4:].sum()
    threshold = 0

    # 幫助有限
# ------------------------------------------------------------------------------------------------------------------
    # 市值營收比 Price-to-Sales Ratio PSR
    當月營收 = data.get('當月營收', 4) * 1000
    當季營收 = 當月營收.iloc[-4:].sum()  # 取近4個月營收總和，當作一季的月營收（4也可以改變）
    市值營收比 = cap / 當季營收


# ------------------------------------------------------------------------------------------------------------------
    # RSV
    days = 90
    rsv = (close.iloc[-1] - close.iloc[-days:].min()) / (close.iloc[-days:].max() - close.iloc[-days:].min())

# ------------------------------------------------------------------------------------------------------------------
    # rade Value
    value = data.get('成交金額', 60) * 1000

# ------------------------------------------------------------------------------------------------------------------
   # 營業利益成長率  (比去年營利好，表示體質變好)
    營業利益 = data.get('營業利益損失', 5)
    營業利益成長率 = (營業利益.iloc[-1] / 營業利益.iloc[-5] - 1) * 100


# ------------------------------------------------------------------------------------------------------------------
    # 股東權益報酬率 ROE (用多少錢賺多少錢)
    稅後淨利 = data.get('本期淨利淨損', 1)
    權益總計 = data.get('權益總計', 1)
    權益總額 = data.get('權益總額', 1)
    權益總計.fillna(權益總額, inplace=True)
    ROE = 稅後淨利.iloc[-1] / 權益總計.iloc[-1]

    # 幫助有限
# ------------------------------------------------------------------------------------------------------------------
    # 月營收
    rev = data.get('當月營收', 13)


    # 幫助有限
    # ------------------------------------------------------------------------------------------------------------------
    con1 = cap <= 2e9  # 3e9, 公司數：168~264, 360%
    con2 = rev[-3:].mean() > rev.mean()  # 公司數： 209~707, 290% "2"
    con3 = net_flow > threshold  # 0, 公司數：422~640, 160%  "3"
    con4 = rsv > 0.2  # 0.7, 公司數：93~795, 220%  "4" 效果好但選出股數受大盤影響劇烈
    con5 = 市值營收比 < 3.5  # 2.2 , 公司數：158~295, 310% "4"  會跟con1重疊 效果不佳
    con6 = (value.iloc[-5:].mean()) > 1e7  # 2e11, 公司數：89~301, 150%
    #
    # con7 = ROE > 0.01  # 0.05, 公司數： 120~644, 120%  "6" 對小市值公司影響微小
    # con8 = 營業利益成長率 > 25  # 30, 公司數： 175~361, 200%  對小市值公司完全沒用
    # con9 = pb_ratio.iloc[-1] < 1.4  # 1, 公司數：149~324, 200%  對小市值公司完全沒用

    # con = con1 & con4 & con5 & con6 & con7 & con3 & con9
    con = con1 & con2 & con3 & con4 & con6
    stocks = len(con[con])
    if stocks > 8:
        gain = 1
        rsv_threshold = 0.2*(stocks/8) * gain
        print(rsv_threshold)
        if rsv_threshold > 0.85:
            rsv_threshold = 0.85
        con4 = rsv > rsv_threshold
        con = con1 & con2 & con3 & con4 & con6
        # if len(con[con]) > 12:
        #     con = con1 & con2 & con3 & con4 & con5

    return con[con]

# print(top_small_strategy(data))
