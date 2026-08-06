"""
纳指ETF(513300) "MA200 7%网格" 策略回测

核心逻辑（围绕 MA200 做逆势网格）：
  - 计算偏离度 d = price / MA200 - 1
  - 档位 k = round(d / 7%)
      k > 0 : 价格高于MA200，每高 7% 一档 → 卖出一档（减仓）
      k < 0 : 价格低于MA200，每低 7% 一档 → 买入一档（加仓）
  - 目标仓位 = clamp(BASE_POS - k * TRANCHE, 0, 1)
      k=0  (=MA200)   → 50% 中性
      k=-3 (低21%)     → 80%
      k=+3 (高21%)     → 20%
      k=±5             → 0% / 100%
  - 仅在档位 k 变化时调仓（网格只在边界成交，避免每日抖动）

对比：买入持有、V2状态机
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
from datetime import date
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb

# ═══════════════════════════════════════
INIT_CAPITAL = 1_000_000
MA_LONG = 200
BAND_PCT = 0.07      # 每 7% 一档
BASE_POS = 0.50      # MA200 处的基准仓位
TRANCHE = 0.10       # 每档调整 10% 仓位
START = '2021-01-01'
END = '2026-07-21'


def calc_ma(arr, window, i):
    if i < window - 1:
        return np.nan
    return arr[max(0, i - window + 1):i + 1].mean()


def backtest_ma200_grid(df):
    data = df.copy()
    data['_d'] = pd.to_datetime(data['date'])
    mask = (data['_d'] >= pd.Timestamp(START)) & (data['_d'] <= pd.Timestamp(END))
    data = data[mask].sort_values('date').reset_index(drop=True)
    n = len(data)
    if n < MA_LONG + 10:
        return None, None

    close = data['close'].astype(float).values
    ma200 = np.array([calc_ma(close, MA_LONG, i) for i in range(n)])

    shares = 0.0
    cash = float(INIT_CAPITAL)
    cur_k = 0
    trades = []
    equity = np.zeros(n)
    pos_series = np.zeros(n)

    def pv(i):
        return shares * close[i] + cash

    # 初始化：第一根有效K线建基准仓位
    start_i = MA_LONG + 5
    init_px = close[start_i]
    shares = INIT_CAPITAL * BASE_POS / init_px
    cash = INIT_CAPITAL - shares * init_px
    cur_k = 0
    pos_series[start_i] = BASE_POS

    for i in range(start_i, n):
        c = close[i]
        m = ma200[i]
        if np.isnan(m) or m <= 0:
            equity[i] = pv(i)
            pos_series[i] = pos_series[i - 1] if i > 0 else BASE_POS
            continue

        d = c / m - 1.0
        k = int(round(d / BAND_PCT))

        if k != cur_k:
            # 调仓到目标仓位
            target_pos = float(np.clip(BASE_POS - k * TRANCHE, 0.0, 1.0))
            value = pv(i)
            # 平仓旧头寸
            if shares > 0:
                cash += shares * c
                shares = 0
            # 建目标仓位
            target_amt = value * target_pos
            if target_amt > 0 and c > 0:
                shares = target_amt / c
                cash = value - target_amt
            action = "SELL" if k > cur_k else "BUY"
            trades.append({
                "date": str(data['date'].iloc[i]),
                "action": action,
                "k": k,
                "dev_pct": d * 100,
                "target_pos": target_pos * 100,
                "n_shares": shares,
                "value": value,
            })
            cur_k = k

        equity[i] = pv(i)
        pos_series[i] = (shares * c) / pv(i) * 100 if pv(i) > 0 else 0

    # 填充前段（MA200预热期）用初始资金
    equity[:start_i] = INIT_CAPITAL

    return equity, trades, pos_series


def calc_metrics(equity):
    if equity is None or len(equity) < 2:
        return {}
    total_ret = (equity[-1] / INIT_CAPITAL - 1) * 100
    peak = np.maximum.accumulate(equity)
    dd = (peak - equity) / peak * 100
    max_dd = dd.max()
    years = len(equity) / 252
    cagr = ((equity[-1] / INIT_CAPITAL) ** (1 / years) - 1) * 100 if years > 0 else 0
    dr = equity[1:] / equity[:-1] - 1
    sharpe = dr.mean() / dr.std() * np.sqrt(252) if dr.std() > 0 else 0
    calmar = cagr / max_dd if max_dd > 0 else 0
    return {"total_return": total_ret, "max_drawdown": max_dd,
            "cagr": cagr, "sharpe": sharpe, "calmar": calmar}


def main():
    sql = """
        SELECT code, date, open, close, high, low, volume
        FROM fund_etf_hist_em
        WHERE date >= '2019-01-01' AND date <= '2026-07-21'
        AND code = '513300'
        ORDER BY code, date
    """
    raw = pd.read_sql(sql, con=mdb.engine())

    print()
    print("=" * 80)
    print("  纳指ETF(513300) MA200 7%网格 策略回测")
    print(f"  参数: 基准仓位={BASE_POS:.0%}  每档={BAND_PCT:.0%}  每档调整={TRANCHE:.0%}")
    print("=" * 80)
    print()

    data = raw[raw['code'] == '513300'].copy()
    eq, trades, pos = backtest_ma200_grid(data)
    m = calc_metrics(eq)

    # 买入持有
    data['_d'] = pd.to_datetime(data['date'])
    mask = (data['_d'] >= pd.Timestamp(START)) & (data['_d'] <= pd.Timestamp(END))
    period = data[mask].sort_values('date')
    bh_start = float(period.iloc[0]['close'])
    bh_end = float(period.iloc[-1]['close'])
    bh_return = (bh_end / bh_start - 1) * 100
    bh_prices = period['close'].astype(float).values
    bh_peak = np.maximum.accumulate(bh_prices)
    bh_dd = (bh_peak - bh_prices) / bh_peak * 100
    bh_max_dd = bh_dd.max()

    print(f"  策略收益:        {m['total_return']:>+8.1f}%")
    print(f"  买入持有:        {bh_return:>+8.1f}%")
    print(f"  差值:            {m['total_return'] - bh_return:>+8.1f}%")
    print(f"  最大回撤(策略):  {m['max_drawdown']:>8.1f}%    BH: {bh_max_dd:>8.1f}%")
    print(f"  年化/Sharpe/Calmar: {m['cagr']:.1f}% / {m['sharpe']:.2f} / {m['calmar']:.2f}")
    print(f"  成交次数:        {len(trades)}")
    print()

    # 买卖统计
    buys = [t for t in trades if t['action'] == 'BUY']
    sells = [t for t in trades if t['action'] == 'SELL']
    print(f"  买入档: {len(buys)} 次   卖出档: {len(sells)} 次")
    print()
    print("  成交明细:")
    for t in trades:
        print(f"    {t['date']}  {t['action']:<4s}  档位k={t['k']:+d}  偏离{t['dev_pct']:+7.1f}%  →仓位{t['target_pos']:5.1f}%")
    print()

    # 与已知结果对比
    print("=" * 80)
    print("  对比汇总（纳指ETF 513300, 2021~2026.07）")
    print("=" * 80)
    print(f"  {'策略':<18s} {'收益':>10s} {'回撤':>10s}")
    print(f"  {'─'*42}")
    print(f"  {'买入持有':<18s} {bh_return:>+9.1f}% {bh_max_dd:>9.1f}%")
    print(f"  {'V2状态机':<18s} {123.6:>+9.1f}% {19.6:>9.1f}%")
    print(f"  {'MA200网格(7%)':<18s} {m['total_return']:>+9.1f}% {m['max_drawdown']:>9.1f}%")
    print()


if __name__ == '__main__':
    main()
