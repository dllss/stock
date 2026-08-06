"""策略择时 vs 买入持有 收益对比"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd, numpy as np
import logging; logging.basicConfig(level=logging.WARNING)
import instock.lib.database as mdb
from instock.backtest.backtest_runner import run_backtest, DEFAULT_SELL_PARAMS

sql = """SELECT code, date, open, close, high, low, volume, quote_change AS p_change
    FROM fund_etf_hist_em
    WHERE date >= '2022-01-01' AND date <= '2026-07-21'
    AND code IN ('512890','513300','513500')
    ORDER BY code, date"""
df = pd.read_sql(sql, con=mdb.engine())

sell_params = DEFAULT_SELL_PARAMS.copy()
sell_params.update({
    'stop_loss': -0.10, 'stop_profit': 0.25,
    'trailing_stop': -0.08, 'trailing_stop_activation': 0.10,
    'max_hold_days': 120, 'cooldown_days': 20,
})

configs = [
    ('513300', '纳斯达克', 'vol_targeting'),
    ('512890', '红利低波', 'bollinger_reversion'),
    ('513500', '标普500', 'ma200_trend'),
]

print()
print('=' * 92)
print(f'  策略择时 vs 买入持有 收益对比 — 2023.01.01 ~ 2026.07.21 (3.55年)')
print('=' * 92)
print()
print(f'  {"ETF":<18s} {"策略":<12s} {"策略收益":>10s} {"买入持有":>10s} {"超额收益":>10s} {"年化超额":>10s}')
print('  ' + '-' * 90)

total_strategy_prod = 1.0
total_buyhold_prod = 1.0

from datetime import date as dt_date
START = dt_date(2023, 1, 1)
END = dt_date(2026, 7, 21)
YEARS = 3.55

for code, name, sid in configs:
    data = df[df['code'] == code].sort_values('date').reset_index(drop=True)

    # 买入持有
    mask = [(d >= START) & (d <= END) for d in data['date']]
    period = data[mask]
    start_p = float(period.iloc[0]['close'])
    end_p = float(period.iloc[-1]['close'])
    bh_return = (end_p / start_p - 1) * 100

    # 策略复利
    data['date'] = data['date'].astype(str).str[:10]
    trades = run_backtest({code: data}, '2023-01-01', '2026-07-21', sid, sell_params)
    compound = 1.0
    for t in trades:
        compound *= (1 + t.profit_pct / 100)
    strategy_return = (compound - 1) * 100

    excess = strategy_return - bh_return
    annual_excess = (compound / (end_p / start_p)) ** (1 / YEARS) - 1
    annual_excess_pct = annual_excess * 100

    total_strategy_prod *= compound
    total_buyhold_prod *= (end_p / start_p)

    print(f'  {name:<18s} {sid:<12s} {strategy_return:>+9.1f}% {bh_return:>+9.1f}% {excess:>+9.1f}% {annual_excess_pct:>+9.1f}%')

print('  ' + '-' * 90)

# 等权组合（100万平分3份，各自买入持有 vs 各自策略）
total_strategy = (total_strategy_prod - 1) * 100
total_buyhold = (total_buyhold_prod - 1) * 100
total_excess = total_strategy - total_buyhold
total_annual = (total_strategy_prod / total_buyhold_prod) ** (1 / YEARS) - 1
total_annual_pct = total_annual * 100

print(f'  {"三ETF等权组合":<18s} {"":<12s} {total_strategy:>+9.1f}% {total_buyhold:>+9.1f}% {total_excess:>+9.1f}% {total_annual_pct:>+9.1f}%')

# 绝对金额（100万本金）
print()
print('  「手握100万，两种做法的差距」')
print('  ' + '-' * 70)
print(f'  {"":<20s} {"金额":>12s} {"vs买入持有":>12s}')
strat_val = 1_000_000 * total_strategy_prod
bh_val = 1_000_000 * total_buyhold_prod
print(f'  {"策略择时(复利)":<20s} {strat_val:>12,.0f}  ─')
print(f'  {"买入持有不动":<20s} {bh_val:>12,.0f}  ─')
print(f'  {"额外多赚":<20s} {strat_val - bh_val:>+12,.0f}  多{(strat_val/bh_val-1)*100:.1f}%')
print()
print(f'  💡 结论: 策略择时比买入持有，多赚约 {total_excess:.0f}%，年化超额 {total_annual_pct:.1f}%')
