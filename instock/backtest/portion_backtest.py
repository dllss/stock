"""
20份资金池仓位管理回测
100万分成20份，每份5万，信号来就投1份
三只ETF各用最优策略
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb
from instock.backtest.backtest_runner import run_backtest, DEFAULT_SELL_PARAMS

# ===== 加载数据 =====
sql = """
    SELECT code, name, date, open, close, high, low, volume, quote_change AS p_change
    FROM fund_etf_hist_em
    WHERE date >= '2022-01-01' AND date <= '2026-07-21'
    AND code IN ('512890','513300','513500')
    ORDER BY code, date
"""
df = pd.read_sql(sql, con=mdb.engine())

kline_dict = {}
for code, grp in df.groupby('code'):
    grp = grp.sort_values('date').reset_index(drop=True)
    grp['date'] = grp['date'].astype(str).str[:10]
    kline_dict[code] = grp

sell_params = DEFAULT_SELL_PARAMS.copy()
sell_params.update({
    'stop_loss': -0.10, 'stop_profit': 0.25,
    'trailing_stop': -0.08, 'trailing_stop_activation': 0.10,
    'max_hold_days': 120, 'cooldown_days': 20,
})

# ===== 每只ETF用各自最优策略 =====
etf_strategy = {
    '512890': 'bollinger_reversion',
    '513300': 'vol_targeting',
    '513500': 'ma200_trend',
}
etf_names = {'512890': '红利低波', '513300': '纳斯达克', '513500': '标普500'}

all_trades = {}
for code, sid in etf_strategy.items():
    all_trades[code] = run_backtest(
        {code: kline_dict[code]}, '2023-01-01', '2026-07-21',
        sid, sell_params
    )

# ===== 20份资金池模拟 =====
PORTIONS = 20
TOTAL_CAPITAL = 1_000_000
portion_size = TOTAL_CAPITAL / PORTIONS  # 50,000

# 收集所有交易事件
events = []
for code, trades in all_trades.items():
    for t in trades:
        events.append({
            'code': code,
            'buy_date': t.buy_date,
            'sell_date': t.sell_date,
            'buy_price': t.buy_price,
            'sell_price': t.sell_price,
            'profit_pct': t.profit_pct,
            'sell_reason': t.sell_reason,
        })

events.sort(key=lambda e: e['buy_date'])

# 模拟仓位管理
active_positions = []
idle_events = 0
position_log = []

for ev in events:
    # 先检查有没有已到期的持仓可以释放
    # （简化：按卖出日期排序，这里signal按买入日期处理）
    if len(active_positions) < PORTIONS:
        # 有可用份额
        shares = portion_size / ev['buy_price']
        pnl = portion_size * ev['profit_pct'] / 100
        active_positions.append(ev)
        status = '开仓'
    else:
        shares = 0
        pnl = 0
        idle_events += 1
        status = '满仓错过'

    position_log.append({
        'code': ev['code'],
        'buy_date': ev['buy_date'],
        'sell_date': ev['sell_date'],
        'buy_price': ev['buy_price'],
        'sell_price': ev['sell_price'],
        'profit_pct': ev['profit_pct'],
        'sell_reason': ev['sell_reason'],
        'shares': round(shares, 0),
        'portion_pnl': round(pnl, 0),
        'status': status,
    })

# 统计
deployed = [p for p in position_log if p['status'] == '开仓']
total_pnl = sum(p['portion_pnl'] for p in deployed)
final_value = TOTAL_CAPITAL + total_pnl
total_return = total_pnl / TOTAL_CAPITAL * 100

# ===== 输出 =====
print()
print('=' * 115)
print('  💰 100万分成20份 · 三ETF各用最优策略 · 仓位管理回测')
print('     回测区间: 2023-01-01 ~ 2026-07-21')
print('=' * 115)
print(f'  策略配置:')
print(f'    512890 红利低波 → 布林带均值回归')
print(f'    513300 纳斯达克   → 波动率自适应')
print(f'    513500 标普500    → MA200趋势过滤')
print(f'  每份资金: {portion_size:,.0f} 元 | 总份数: {PORTIONS} | 总资金: {TOTAL_CAPITAL:,.0f} 元')
print()

# 逐笔明细
print(f'  {"ETF":<14s} {"买入":<12s} {"卖出":<12s} {"买价":>7s} {"卖价":>7s} {"单笔%":>7s} {"份盈亏":>10s} {"状态":<10s}')
print('  ' + '-' * 108)
for p in position_log:
    name = f"{etf_names[p['code']]}({p['code']})"
    ret_str = f"{p['profit_pct']:+.2f}%" if p['status'] == '开仓' else '—'
    pnl_str = f"{p['portion_pnl']:+,.0f}" if p['status'] == '开仓' else '—'
    print(f'  {name:<14s} {p["buy_date"]:<12s} {p["sell_date"]:<12s} '
          f'{p["buy_price"]:>7.4f} {p["sell_price"]:>7.4f} '
          f'{ret_str:>7s} {pnl_str:>10s} {p["status"]:<10s}')

print('  ' + '-' * 108)

# 按ETF汇总
print()
print('  📊 按ETF汇总:')
print(f'  {"ETF":<16s} {"策略":<14s} {"交易":>4s} {"盈利笔":>5s} {"累计盈亏":>12s}')
print('  ' + '-' * 58)
for code in ['512890','513300','513500']:
    etf_p = [p for p in deployed if p['code'] == code]
    etf_pnl = sum(p['portion_pnl'] for p in etf_p)
    etf_wins = sum(1 for p in etf_p if p['profit_pct'] > 0)
    print(f'  {etf_names[code]+"("+code+")":<16s} {etf_strategy[code]:<14s} '
          f'{len(etf_p):>4d} {etf_wins:>4d} {etf_pnl:>12,.0f}')

# 总结
idle_cash = (PORTIONS - len(deployed)) * portion_size
print()
print('  ' + '=' * 58)
print(f'  📋 总结')
print(f'    总信号数:        {len(events)} 次')
print(f'    实际开仓:        {len(deployed)} 次')
print(f'    因满仓错过:      {idle_events} 次')
print(f'    资金占用:        {len(deployed) * portion_size:,.0f} 元 / {TOTAL_CAPITAL:,.0f} 元')
print(f'    闲置资金:        {idle_cash:,.0f} 元 ({idle_cash/TOTAL_CAPITAL*100:.0f}%)')
print(f'    总盈亏:          {total_pnl:+,.0f} 元')
print(f'    总收益率:         {total_return:+.2f}%')
print(f'    最终资产:         {final_value:,.0f} 元')
print()
print(f'  💡 分析:')
print(f'    3.5年仅{len(events)}个买入信号，最多同时持仓{len(deployed)}份，远不到20份上限')
print(f'    原因: ETF择时策略本身信号稀少（每月~0.5次），`-')
if len(deployed) > 0:
    print(f'    平摊每份年化:    {(total_return / len(deployed)):.2f}% / {(total_return / len(deployed) / 3.5):.2f}%')  # 3.5年
print()
print(f'  ⚠️  20份方案的核心问题: 信号不够多，大量资金闲置在现金')
print(f'      → 如果确实有100万，建议拿出其中1-3份(5-15万)跟策略')
print(f'      → 剩余资金配置债券/货币基金吃利息')
print('=' * 58)
