"""不同cooldown/max_hold参数下的信号数量对比"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb
from instock.backtest.backtest_runner import run_backtest, DEFAULT_SELL_PARAMS

sql = """
    SELECT code, date, open, close, high, low, volume, quote_change AS p_change
    FROM fund_etf_hist_em
    WHERE date >= '2022-01-01' AND date <= '2026-07-21'
    AND code = '513300'
    ORDER BY date
"""
df = pd.read_sql(sql, con=mdb.engine())
df = df.sort_values('date').reset_index(drop=True)
df['date'] = df['date'].astype(str).str[:10]
kline = {'513300': df}

configs = [
    ("保守(当前)", 20, 120, -0.10, 0.25),
    ("中频",       10, 60,  -0.08, 0.15),
    ("高频",        5, 30,  -0.05, 0.10),
    ("激进",        3, 20,  -0.03, 0.08),
]

print()
print(f'{"="*90}')
print(f'  513300 纳斯达克 · 波动率自适应 · 不同频率参数对比')
print(f'  回测区间: 2023-01-01 ~ 2026-07-21')
print(f'{"="*90}')
print(f'  {"版本":<14s} {"冷却":>5s} {"最大持仓":>7s} {"止损":>6s} {"止盈":>6s} {"交易数":>6s} {"胜率":>6s} {"总收益":>8s}')
print(f'  {"-"*70}')

for name, cd, mh, sl, sp in configs:
    sparam = DEFAULT_SELL_PARAMS.copy()
    sparam.update({
        'stop_loss': sl, 'stop_profit': sp,
        'trailing_stop': max(sl, -0.08),
        'trailing_stop_activation': min(sp, 0.10),
        'max_hold_days': mh, 'cooldown_days': cd,
    })
    trades = run_backtest(kline, '2023-01-01', '2026-07-21', 'vol_targeting', sparam)
    wins = sum(1 for t in trades if t.profit_pct > 0)
    wr = wins/len(trades)*100 if trades else 0
    total = sum(t.profit_pct for t in trades)
    print(f'  {name:<14s} {cd:>5d}天 {mh:>6d}天 {sl*100:>+5.0f}% {sp*100:>+5.0f}% {len(trades):>6d} {wr:>5.0f}% {total:>+7.1f}%')

print(f'{"="*90}')
print(f'  核心逻辑: 冷却越短 + 持仓越短 → 信号越多 → 但胜率可能下降')
print(f'  ETF天生波动小，不宜把止损设太紧（容易被正常回撤震出去）')
print(f'{"="*90}')
