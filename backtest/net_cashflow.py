import pandas as pd
import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
from data import Data

data = Data()
data.date = datetime.date(2021,4,12)
close = data.get('收盤價', 1000)
# 將每季累計的財務數據，轉換成單季
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


# 計算自由現金流 = 營業活動之淨現金流入 - 投資活動之淨現金流出
投資現金流 = toSeasonal(data.get('投資活動之淨現金流入流出', 4))
營業現金流 = toSeasonal(data.get('營業活動之淨現金流入流出', 4))
net_flow = (投資現金流 + 營業現金流).iloc[-4:].sum()
# net_flow.describe()
threshold = 0
con1 = net_flow > threshold
con2 = net_flow <= threshold

#  =============================簡單回測======================================
data.date = datetime.date(2020,4,11)

end_date = close.index[-1]
try:
    close = close.loc[data.date:end_date]
except KeyError:
    close = close.loc[data.date+timedelta(days=2):end_date]

print('origin stocks number:', [len(con1[con1]), len(con2[con2])])

flow_list1 = []
for i in con1[con1].index:
    if i in close.columns:
        flow_list1.append(i)

flow_list2 = []
for i in con2[con2].index:
    if i in close.columns:
        flow_list2.append(i)

nums = [len(flow_list1), len(flow_list2)]

net_con1 = close[flow_list1].mean(axis=1)
net_con2 = close[flow_list2].mean(axis=1)
(net_con1 / net_con1[0]).plot(label=f'net_flow > {threshold}|stocks:{nums[0]}')
(net_con2 / net_con2[0]).plot(label=f'net_flow <= {threshold} |stocks:{nums[1]}')

plt.title('TW stocks net_flow comparison')
plt.xlabel('time')
plt.ylabel('price growth ratio')
plt.legend()
plt.grid()
plt.show()