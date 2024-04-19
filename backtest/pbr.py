import datetime
import matplotlib.pyplot as plt
from backtest.data import Data

data = Data()
data.date = datetime.date(2024,4,10)
close = data.get('收盤價', 3000)

def pb_ratios(n, data): # 最近n季
    歸屬業主權益 = data.get('歸屬於母公司業主之權益合計', n)
    權益總計 = data.get('權益總計', n)
    權益總額 = data.get('權益總額', n)
    sc = data.get('股本合計', n)
    歸屬業主權益.fillna(權益總計, inplace=True)
    歸屬業主權益.fillna(權益總額, inplace=True)
    close = data.get('收盤價', 100*n)

    pb_ratio = close.reindex(sc.index, method='ffill') / (歸屬業主權益 / sc) / 10
    pb_ratio['6531'] = pb_ratio['6531']*2

    return pb_ratio


pbs = pb_ratios(16, data)
con1 = (pbs.iloc[0] < 0.7) & (pbs.iloc[0] > 0)
con2 = (pbs.iloc[0] < 1.4) & (pbs.iloc[0] >= 0.7)
con3 = (pbs.iloc[0] < 2.1) & (pbs.iloc[0] >= 1.4)
con4 = (pbs.iloc[0] < 2.8) & (pbs.iloc[0] >= 2.1)
con5 = pbs.iloc[0] >= 2.8
nums = [len(con1[con1]), len(con2[con2]), len(con3[con3]), len(con4[con4]), len(con5[con5])]

#  =============================簡單回測======================================
# company_amount = [len()]
end_time = close.index[-1]
price_con1 = close.loc[pbs.index[0]:end_time][con1[con1].index].mean(axis=1)
price_con2 = close.loc[pbs.index[0]:end_time][con2[con2].index].mean(axis=1)
# price_con = close.loc[pbs.index[0]:end_time][pbs.columns[con2]].mean(axis=1)  # 以columns選，結果同上
price_con3 = close.loc[pbs.index[0]:end_time][con3[con3].index].mean(axis=1)
price_con4 = close.loc[pbs.index[0]:end_time][con4[con4].index].mean(axis=1)
price_con5 = close.loc[pbs.index[0]:end_time][con5[con5].index].mean(axis=1)
(price_con1/price_con1.iloc[0]).plot(label=f'pb ratio < 0.7 & > 0 |stocks:{nums[0]}')
(price_con2/price_con2.iloc[0]).plot(label=f'pb ratio < 1.4 & >= 0.7 |stocks:{nums[1]}')
(price_con3/price_con3.iloc[0]).plot(label=f'pb ratio < 2.1 & >= 1.4 |stocks:{nums[2]}')
(price_con4/price_con4.iloc[0]).plot(label=f'pb ratio < 2.8 & >= 2.1 |stocks:{nums[3]}')
(price_con5/price_con5.iloc[0]).plot(label=f'pb ratio > 2.8 |stocks:{nums[4]}')
plt.title('TW stocks pb_ratio comparison')
plt.xlabel('time')
plt.ylabel('price growth ratio')
plt.legend()
plt.grid()
plt.show()
