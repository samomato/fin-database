import datetime
import matplotlib.pyplot as plt
from backtest.data import Data

data = Data()
data.date = datetime.date(2018,12,11)
close = data.get('收盤價', 1000)

# n 最近n個月的平均大於最近m個月的平均


end_date = close.index[-1]
data.date = datetime.date(2018,6,12)

def rev_month_increase(n, m):
    rev = data.get('當月營收', m)
    con = rev[-n:].mean() > rev.mean()
    return rev.columns[con]

def rev_month_yoy():
    rev = data.get('當月營收', 13)
    # con1 = (rev.iloc[-1] > rev.iloc[-2])
    con2 = (rev.iloc[-1] > rev.iloc[-13]*1.2)
    con3 = rev[-3:].mean() > rev.mean()*1.1
    con = con2 & con3
    return rev.iloc[-1][con].index


#  =============================簡單回測======================================
rev_list1 = []
rev_list2 = []
price_df = close.loc[f'{data.date}':end_date]
for i in rev_month_increase(3, 12):
    if i in price_df.columns:
        rev_list1.append(i)

for i in rev_month_yoy():
    if i in price_df.columns:
        rev_list2.append(i)

price_nocon = price_df.mean(axis=1)
price_con1 = (price_df[rev_list1]).mean(axis=1)
price_con2 = (price_df[rev_list2]).mean(axis=1)
nums = [len(close.loc[f'{data.date}']), len(rev_list1), len(rev_list2)]
(price_nocon/price_nocon.iloc[0]).plot(label=f'All stocks |stocks:{nums[0]}')

(price_con1/price_con1.iloc[0]).plot(label=f'recent 3 M > 1 Y |stocks:{nums[1]}')

(price_con2/price_con2.iloc[0]).plot(label=f' 3M > 1Y & +YoY *1.2 |stocks:{nums[2]}')

plt.title('TW stocks month rev comparison')
plt.xlabel('time')
plt.ylabel('price growth ratio')
plt.legend()
plt.grid()
plt.show()
