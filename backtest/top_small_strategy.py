import datetime
import pandas as pd
import matplotlib.pyplot as plt
from backtest.data import Data

data = Data()
data.date = datetime.date(2024,4,10)
# # 股本 share capital
# sc = data.get('股本合計', 1)
# # print('sc', sc)
# # 拿365天的股價
# close = data.get('收盤價', 400)
# open = data.get('開盤價', 365)
# high = data.get('最高價', 365)
# low = data.get('最低價', 365)

# # conditions:
# # 1. 市值 Market cap： 股本(x 1000 證交所單位為千元)/面額 * 股價
# # 面額： 6415 矽力:2.5  ; 6548 長科: 0.4 ; 6531 愛普: 5
# sc = sc.iloc[-1]
# mc = sc * 1000 / 10 * close.iloc[-1]
# mc['6531'] = mc['6531'] * (10 / 5)
# mc = mc.dropna()
# con1 = mc < 1e10


# 2. 股價淨值比 = 股價/每股淨值 ;  每股淨值 = 股東權益/流通股數; 流通股數 = 股本/面額
# 股價淨值比 = 股價/(股東權益/(股本/面額)) = 股價/ (股東權益/股本) / 面額
# def pb_ratios(n, _data):  # 最近n季
#     歸屬業主權益 = _data.get('歸屬於母公司業主之權益合計', n)
#     權益總計 = _data.get('權益總計', n)
#     權益總額 = _data.get('權益總額', n)
#     _sc = _data.get('股本合計', n)
#     歸屬業主權益.fillna(權益總計, inplace=True)
#     歸屬業主權益.fillna(權益總額, inplace=True)
#     _close = _data.get('收盤價', 100*n)
#     _pb_ratio = _close.reindex(_sc.index, method='ffill') / (歸屬業主權益 / sc) / 10
#     _pb_ratio['6531'] = _pb_ratio['6531']*2
#     return _pb_ratio
#
#
# pb_ratio = pb_ratios(1, data)
# con2 = pb_ratio.iloc[-1] < 0.7




def top_small_strategy(data):
    close = data.get('收盤價', 280)
# ------------------------------------------------------------------------------------------------------------------
    #  condition 1. 市值
    sc = data.get('股本合計', 1)
    cap = sc.iloc[0] * 1000 / 10 * close.reindex(sc.index, method='ffill').iloc[0]  # 市值
    cap['6531'] = cap['6531'] * (10 / 5)
    con1 = cap <= 1e10
# ------------------------------------------------------------------------------------------------------------------
    # condition 2. 股價淨值比 Price to book ratio
    歸屬業主權益 = data.get('歸屬於母公司業主之權益合計', 1)
    權益總計 = data.get('權益總計', 1)
    權益總額 = data.get('權益總額', 1)
    sc = data.get('股本合計', 1)
    歸屬業主權益.fillna(權益總計, inplace=True)
    歸屬業主權益.fillna(權益總額, inplace=True)
    # pb_ratio = close.reindex(sc.index, method='ffill') / (歸屬業主權益 / sc) / 10
    pb_ratio = close.iloc[-1] / (歸屬業主權益 / sc) / 10  # 用最近的股價去算pb ratio 較不失真 比免股票這段期間暴漲
    pb_ratio['6531'] = pb_ratio['6531']*2
    con2 = pb_ratio.iloc[-1] < 2
# ------------------------------------------------------------------------------------------------------------------
    # condition 3. 自由現金流 net cash flow
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
    con3 = net_flow > threshold
    # con = net_flow <= threshold
# ------------------------------------------------------------------------------------------------------------------

    con = con1 & con2 &con3
    return con[con]


print(top_small_strategy(data))
