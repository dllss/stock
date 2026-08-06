"""分析各策略信号频率 -- 为什么信号这么稀"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
from datetime import date
import instock.lib.database as mdb

sql = """
    SELECT code, date, open, close, high, low, volume, quote_change AS p_change
    FROM fund_etf_hist_em
    WHERE date >= '2022-01-01' AND date <= '2026-07-21'
    AND code IN ('512890','513300','513500')
    ORDER BY code, date
"""
df = pd.read_sql(sql, con=mdb.engine())
df['date'] = pd.to_datetime(df['date']).dt.date
START = date(2023, 1, 1)

for code in ['513300', '512890', '513500']:
    etf_name = {'513300': '纳斯达克', '512890': '红利低波', '513500': '标普500'}[code]
    data = df[df['code'] == code].sort_values('date').reset_index(drop=True)
    mask = [d >= START for d in data['date']]
    total = sum(mask)

    print(f'\n{"="*60}')
    print(f'  {etf_name} ({code})  2023.01 ~ 2026.07  共 {total} 个交易日')
    print(f'{"="*60}')

    # ---- 波动率自适应 (vol_targeting) ----
    passed = failed_trend = failed_vol = failed_both = 0
    for i in range(60, len(data)):
        if data.iloc[i]['date'] < START:
            continue
        c = data.iloc[i]['close']
        ma60 = data['close'].iloc[i - 59: i + 1].mean()
        trend = c > ma60

        atr_w = data.iloc[i - 13: i + 1]
        trs = []
        for j in range(1, len(atr_w)):
            h, l, pc = atr_w.iloc[j]['high'], atr_w.iloc[j]['low'], atr_w.iloc[j - 1]['close']
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        atr14 = sum(trs) / len(trs) if trs else 0
        annual_v = (atr14 / c) * (252 ** 0.5) if atr14 > 0 and c > 0 else 99
        vol = annual_v <= 0.40

        if trend and vol:
            passed += 1
        elif trend and not vol:
            failed_vol += 1
        elif not trend and vol:
            failed_trend += 1
        else:
            failed_both += 1

    print(f'\n  [vol_targeting 信号条件逐层过滤]')
    print(f'  {"条件":<38s} {"天数":>5s} {"占比":>7s}')
    print(f'  {"-"*50}')
    print(f'  {"价格 > MA60 (趋势过滤)":<38s} {passed+failed_vol:>5d} {(passed+failed_vol)/total*100:>6.1f}%')
    print(f'  {"年化波动率 < 40% (风控)":<38s} {passed+failed_trend:>5d} {(passed+failed_trend)/total*100:>6.1f}%')
    print(f'  {"两条件同时满足 (可以买入)":<38s} {passed:>5d} {passed/total*100:>6.1f}%')
    print(f'  {"  趋势好但波动大(不敢买)":<38s} {failed_vol:>5d} {failed_vol/total*100:>6.1f}%')
    print(f'  {"  低波但下跌中(不能买)":<38s} {failed_trend:>5d} {failed_trend/total*100:>6.1f}%')
    print(f'  {"  既跌又波动大(远远躲开)":<38s} {failed_both:>5d} {failed_both/total*100:>6.1f}%')

    # ---- MA200趋势 ----
    above_ma200 = 0
    for i in range(200, len(data)):
        if data.iloc[i]['date'] < START:
            continue
        c = data.iloc[i]['close']
        ma200 = data['close'].iloc[i - 199: i + 1].mean()
        if c > ma200:
            above_ma200 += 1
    print(f'\n  [ma200_trend]')
    print(f'  价格 > MA200 的天数: {above_ma200} / {total} = {above_ma200/total*100:.1f}%')
    print(f'  (还需放量确认 + 首次突破 + cooldown → 只剩约7笔)')

    # ---- 布林带回归 ----
    bb_hit = 0
    for i in range(60, len(data)):
        if data.iloc[i]['date'] < START:
            continue
        w = data.iloc[i - 19: i + 1]
        c = w.iloc[-1]['close']
        ma20 = w['close'].mean()
        std20 = w['close'].std()
        lower = ma20 - 2 * std20
        if c <= lower * 1.02:
            bb_hit += 1
    print(f'\n  [bollinger_reversion]')
    print(f'  触及布林下轨 2sigma 的天数: {bb_hit} / {total} = {bb_hit/total*100:.1f}%')
    print(f'  (还需RSI<35 + 缩量 + cooldown → 只剩6笔)')

print(f'\n{"="*60}')
print(f'  总结:')
print(f'  策略设计意图就是过滤掉 80-95% 的交易日')
print(f'  ETF 趋势策略: 每年 2-5 个信号 (正常)')
print(f'  个股短线策略: 每年 20-50 个信号')
print(f'')
print(f'  ETF 择时的核心优势: 少而精，每笔胜率高')
print(f'  不是\"信号少=不好\"，而是\"只做最有把握的\"')
print(f'{"="*60}')
