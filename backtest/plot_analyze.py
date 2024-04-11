from data import Data
import matplotlib.pyplot as plt

data = Data()
amount = 5000
tw_amount = round(amount*0.97)
linewidth = 0.8
#  國際股
sp500tr = data.get('sp500tr', amount)
qqq = data.get('qqq', amount)
ivv = data.get('ivv', amount)
vti = data.get('vti', amount)
ijh = data.get('ijh', amount)
# mchi = data.get('mchi', amount)
tlt = data.get('tlt', amount)
bnd = data.get('bnd', amount)
vix = data.get('vix_close', amount)

# 台股
tw50i_re = data.get('tw50i_re', tw_amount)
tw100i_re = data.get('tw100i_re', tw_amount)
light = data.get('light_score', 239)

# ---------------------------------------------------------

#  國際股
sp500tr = sp500tr['sp500tr'] / sp500tr['sp500tr'][0]
qqq = qqq['qqq'] / qqq['qqq'][0]
ivv = ivv['ivv'] / ivv['ivv'][0]
vti = vti['vti'] / vti['vti'][0]
ijh = ijh['ijh'] / ijh['ijh'][0]
# mchi = mchi['mchi'] / mchi['mchi'][0]
tlt = tlt['tlt'] / tlt['tlt'][0]
bnd = bnd['bnd'] / bnd['bnd'][0]
vix = vix['vix_close'] / vix['vix_close'][0]

#  台股
tw50i_re = tw50i_re['tw50i_re'] / tw50i_re['tw50i_re'][0]
tw100i_re = tw100i_re['tw100i_re'] / tw100i_re['tw100i_re'][0]


# 作圖
fig, ax1 = plt.subplots(figsize=(15,8))
# plt.plot(qqq_close.index, qqq_close['qqq'], label='QQQ')
# plt.subplot(211)
# p1 = ax1.plot(sp500tr.index, sp500tr, label='SP500TR')
p2 = ax1.plot(qqq.index, qqq, label='QQQ')
p3 = plt.plot(ivv.index, ivv, label='IVV')
# p4 = plt.plot(vti.index, vti, label='VTI')
# p5 = plt.plot(ijh.index, ijh, label='IJH')
# plt.plot(mchi.index, mchi, label='MCHI')
p6 = ax1.plot(tlt.index, tlt, 'k', label='TLT')
# p7 = ax1.plot(tw50i_re.index, tw50i_re, 'r',label='TW50iTR')
# p8 = plt.plot(tw100i_re.index, tw100i_re, label='TW100iTR')
p9 = plt.plot(bnd.index, bnd, label='BND')
p10 = plt.plot(vix.index, vix, label='VIX')
# plt.setp([p1, p2, p3, p4, p5, p6, p7, p8], 'linewidth', linewidth)
plt.setp([p2, p3, p6, p9, p10], 'linewidth', linewidth)
plt.grid()
plt.yscale('log')
plt.xlabel('Time')
plt.title('Invest tool 5 Years')
plt.legend(loc='upper left')

# # plt.subplot(212)
# ax2 = ax1.twinx()
# pl = ax2.plot(light.index, light, 'o-', label='TW LIGHT')
# plt.setp(pl, 'linewidth', linewidth)
# plt.grid()
# plt.xlabel('Time')
# plt.title('Invest tool 5 Years')
# plt.legend(loc='upper left')
# # plt.plot(ijh_close.index, ijh_close['ijh'], label='IJH')

plt.show()
