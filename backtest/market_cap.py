import datetime
import matplotlib.pyplot as plt
from backtest.data import Data
from datetime import timedelta

data = Data()
data.date = datetime.date(2024,4,10)
close = data.get('收盤價', 5000)
sc = data.get('股本合計', 32)


mc = sc.iloc[0] * 1000 / 10 * close.reindex(sc.index, method='ffill').iloc[0]  # n 季前的所有公司市值
mc['6531'] = mc['6531'] * (10 / 5)  # 6531的面額是5塊
# print(sc.index[0])
mc = mc.dropna()
con1 = mc <= 3e9
con2 = (mc > 3e9) & (mc < 1e10)
con3 = mc >= 1e10
# print(con1[con1].index)


#  =============================簡單回測======================================
end_date = close.index[-1]
try:
    close = close.loc[sc.index[0]:end_date]
except KeyError:
    close = close.loc[sc.index[0]+timedelta(days=2):end_date]

nums = [len(con1[con1]), len(con2[con2]), len(con3[con3])]
mc_con1 = close[con1[con1].index].mean(axis=1)
mc_con2 = close[con2[con2].index].mean(axis=1)
mc_con3 = close[con3[con3].index].mean(axis=1)
(mc_con1/mc_con1[0]).plot(label=f'mc <= 3e9 |stocks:{nums[0]}')
(mc_con2/mc_con2[0]).plot(label=f'(mc > 3e9) & (mc < 1e10) |stocks:{nums[1]}')
(mc_con3/mc_con3[0]).plot(label=f'mc >= 1e10 |stocks:{nums[2]}')

plt.title('TW stocks market cap comparison')
plt.xlabel('time')
plt.ylabel('price growth ratio')
plt.legend()
plt.grid()
plt.show()