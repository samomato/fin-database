from data import Data
import datetime
import pandas as pd
data = Data()
data.date = datetime.date(2024,4,5)
def test_strategy(data):
    股本 = data.get('股本合計', 1)
    price = data.get('收盤價', 100)

    # 目前市值
    cap = 股本.iloc[-1] * 1000 /10 * price.iloc[-1]
    cap = cap.sort_values(ascending=False, na_position='last')
    print(cap.iloc[:50])
    cap.iloc[:50] = True
    print(cap.iloc[:50])
    return cap.iloc[:50]

    # 當天股本 = 股本.iloc[-1]
    # 市值 = 當天股本 * 1000 / 10 * 當天股價
    #print(price)




