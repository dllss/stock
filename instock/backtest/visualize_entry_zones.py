"""可视化各策略的买入区间 — 生成K线图"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import FancyBboxPatch
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb
from instock.backtest.backtest_runner import run_backtest, DEFAULT_SELL_PARAMS, STRATEGY_CONFIGS

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

sql = """
    SELECT code, date, open, close, high, low, volume, quote_change AS p_change
    FROM fund_etf_hist_em
    WHERE date >= '2022-01-01' AND date <= '2026-07-21'
    AND code IN ('512890','513300','513500')
    ORDER BY code, date
"""
df = pd.read_sql(sql, con=mdb.engine())

sell_params = DEFAULT_SELL_PARAMS.copy()
sell_params.update({
    'stop_loss': -0.10, 'stop_profit': 0.25,
    'trailing_stop': -0.08, 'trailing_stop_activation': 0.10,
    'max_hold_days': 120, 'cooldown_days': 20,
})

etf_config = [
    ('513300', 'NASDAQ ETF', 'vol_targeting'),
    ('512890', 'Dividend LowVol', 'bollinger_reversion'),
    ('513500', 'S&P500 ETF', 'ma200_trend'),
]

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'script', 'plots')
os.makedirs(OUTPUT_DIR, exist_ok=True)

for code, name, sid in etf_config:
    data = df[df['code'] == code].sort_values('date').reset_index(drop=True)
    data['date'] = pd.to_datetime(data['date'])
    data_str = data.copy()
    data_str['date'] = data_str['date'].astype(str).str[:10]
    kline = {code: data_str}
    trades = run_backtest(kline, '2023-01-01', '2026-07-21', sid, sell_params)

    if not trades:
        print(f'{name} ({code}) × {sid}: 无交易！')
        continue

    strategy_name = STRATEGY_CONFIGS.get(sid, {}).get('name', sid)

    # 创建图像
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 10),
                                   gridspec_kw={'height_ratios': [3, 1]},
                                   sharex=True)

    # ===== 上轴：K线 + 指标 + 买卖信号 =====
    dates = data['date'].values
    closes = data['close'].astype(float).values
    volumes = data['volume'].astype(float).values

    # 背景色区分交易区间
    for t in trades:
        bd = pd.Timestamp(t.buy_date)
        sd = pd.Timestamp(t.sell_date)
        color = '#e8f5e9' if t.profit_pct > 0 else '#ffebee'
        ax1.axvspan(bd, sd, alpha=0.25, color=color, zorder=0)

    # 收盘价线
    ax1.plot(dates, closes, color='#1565c0', linewidth=1.0, alpha=0.8, label='Close Price', zorder=2)

    # 计算指标
    if sid == 'vol_targeting':
        ma60 = pd.Series(closes).rolling(60).mean().values
        ax1.plot(dates, ma60, color='#e65100', linewidth=1.0, linestyle='--', alpha=0.7, label='MA60')
        # ATR
        high = data['high'].astype(float).values
        low = data['low'].astype(float).values
        tr = np.maximum(high - low, np.maximum(abs(high - np.roll(closes, 1)), abs(low - np.roll(closes, 1))))
        atr14 = pd.Series(tr).rolling(14).mean().values
        annual_vol = (atr14 / closes) * np.sqrt(252) * 100
        ax2.plot(dates, annual_vol, color='#7b1fa2', linewidth=1.0, alpha=0.7, label='Annualized Vol(%)')
        ax2.axhline(y=40, color='red', linewidth=0.8, linestyle=':', alpha=0.6, label='40% Limit')
        ax2.set_ylabel('Volatility %')
        ax2.legend(loc='upper left', fontsize=8)
        ax2.set_ylim(0, max(80, np.nanmax(annual_vol) * 1.1))

    elif sid == 'bollinger_reversion':
        ma20 = pd.Series(closes).rolling(20).mean().values
        std20 = pd.Series(closes).rolling(20).std().values
        upper = ma20 + 2 * std20
        lower = ma20 - 2 * std20
        ax1.plot(dates, ma20, color='#e65100', linewidth=1.0, linestyle='--', alpha=0.7, label='MA20')
        ax1.fill_between(dates, upper, lower, alpha=0.08, color='#e65100', label='Bollinger Band')
        ax1.plot(dates, upper, color='#bf360c', linewidth=0.5, alpha=0.4)
        ax1.plot(dates, lower, color='#bf360c', linewidth=0.5, alpha=0.4)
        # 成交量
        ax2.bar(dates, volumes / 1e6, color='#90caf9', alpha=0.6, width=1.0, label='Volume(M)')
        ax2.set_ylabel('Volume (M)')
        ax2.legend(loc='upper left', fontsize=8)

    elif sid == 'ma200_trend':
        ma200 = pd.Series(closes).rolling(200).mean().values
        ax1.plot(dates, ma200, color='#e65100', linewidth=1.5, linestyle='--', alpha=0.8, label='MA200')
        # 成交量
        ax2.bar(dates, volumes / 1e6, color='#90caf9', alpha=0.6, width=1.0, label='Volume(M)')
        ax2.set_ylabel('Volume (M)')
        ax2.legend(loc='upper left', fontsize=8)

    # 标记买点和卖点
    buy_x, buy_y, sell_x, sell_y = [], [], [], []
    buy_labels, sell_labels = [], []
    for t in trades:
        bd = pd.Timestamp(t.buy_date)
        sd = pd.Timestamp(t.sell_date)
        if bd in dates:
            idx = np.where(dates == bd)[0][0]
            buy_x.append(bd)
            buy_y.append(closes[idx])
            buy_labels.append(f'{t.buy_date[5:]}\n{t.buy_price:.3f}')
        if sd in dates:
            idx = np.where(dates == sd)[0][0]
            sell_x.append(sd)
            sell_y.append(closes[idx])
            sell_labels.append(f'{t.profit_pct:+.1f}%')

    if buy_x:
        ax1.scatter(buy_x, buy_y, marker='^', s=120, c='#2e7d32',
                    edgecolors='white', linewidths=1.0, zorder=10, label='BUY')
        for x, y, lbl in zip(buy_x, buy_y, buy_labels):
            ax1.annotate(lbl, (x, y), textcoords="offset points", xytext=(8, 12),
                        fontsize=7, color='#2e7d32', fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='#e8f5e9', alpha=0.8))

    if sell_x:
        ax1.scatter(sell_x, sell_y, marker='v', s=100, c='#c62828',
                    edgecolors='white', linewidths=1.0, zorder=10, label='SELL')
        for x, y, lbl in zip(sell_x, sell_y, sell_labels):
            ax1.annotate(lbl, (x, y), textcoords="offset points", xytext=(8, -16),
                        fontsize=7, color='#c62828', fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='#ffebee', alpha=0.8))

    # 统计信息
    wins = sum(1 for t in trades if t.profit_pct > 0)
    total_profit = sum(t.profit_pct for t in trades)
    avg_win = np.mean([t.profit_pct for t in trades if t.profit_pct > 0]) if wins > 0 else 0
    avg_loss = np.mean([t.profit_pct for t in trades if t.profit_pct <= 0]) if len(trades) - wins > 0 else 0

    stats_text = (
        f"Total Trades: {len(trades)} | Win Rate: {wins}/{len(trades)} ({wins/len(trades)*100:.0f}%)\n"
        f"Total Return: {total_profit:+.1f}% | Avg Win: {avg_win:+.1f}% | Avg Loss: {avg_loss:+.1f}%"
    )

    ax1.set_title(f'{name} ({code}) — {strategy_name}\n{stats_text}',
                  fontsize=12, fontweight='bold', pad=15)
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left', fontsize=8)
    ax1.grid(True, alpha=0.3)

    # X轴格式
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=8)

    ax2.set_xlabel('Date')
    ax2.grid(True, alpha=0.3)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=8)

    plt.tight_layout()

    filename = f'{code}_{sid}_trades.png'
    filepath = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)

    print(f'✅ {name} ({code}) — {strategy_name}')
    print(f'   图片已保存: {filepath}')
    print(f'   {len(trades)}笔交易 | 胜率 {wins/len(trades)*100:.0f}% | 总收益 {total_profit:+.1f}%')
    print()

print(f'\n所有图表已生成到: {OUTPUT_DIR}')
