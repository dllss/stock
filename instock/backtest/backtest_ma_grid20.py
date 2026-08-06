"""
纳指ETF(513300) "20份×5% 围绕MAn 7%网格" 策略

规则：
  资金分 20 份，每份 = 5% 总资金。
  锚定均线 MAn。偏离度 d = price/MA - 1，档位 k = round(d / 7%)。
  - 价格 = MA (k=0)        → 10 份 = 50% 中性仓位（中点）
  - 每高 MA 7% (k>0)       → 卖出 1 份（减 5%）
  - 每低 MA 7% (k<0)       → 买入 1 份（加 5%）
  仓位份数 = clamp(10 - k, 0, 20)  → 持仓比例 = 份数 × 5%
  仅在档位 k 变化时调仓（网格只在边界成交，避免每日抖动）

扫描锚定均线：MA20 / MA30 / MA60 / MA120 / MA200
对比：单均线趋势跟踪（前一轮）、买入持有
含万3单边手续费。
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb

INIT_CAPITAL = 1_000_000
START = '2022-01-01'
END = '2022-12-31'
FEE = 0.0003
BAND = 0.07
TOTAL_SHARES = 20          # 总份数
EACH = 1.0 / TOTAL_SHARES  # 每份 = 5%
BASE_SHARES = TOTAL_SHARES // 2  # 中点 10 份 = 50%
MA_ANCHORS = [20, 30, 60, 120, 200]


def calc_ma(arr, window):
    return pd.Series(arr).rolling(window).mean().values


def backtest_ma_grid(df, ma_win):
    """围绕 MAn 做 7% 网格，档位变化时调仓"""
    data = df.copy()
    data['_d'] = pd.to_datetime(data['date'])
    full = data.sort_values('date').reset_index(drop=True)
    close_all = full['close'].astype(float).values
    full['ma'] = calc_ma(close_all, ma_win)

    mask = (full['_d'] >= pd.Timestamp(START)) & (full['_d'] <= pd.Timestamp(END))
    period = full[mask].reset_index(drop=True)
    n = len(period)
    close = period['close'].astype(float).values
    ma = period['ma'].astype(float).values
    date = period['date'].values

    shares = 0.0
    cash = float(INIT_CAPITAL)
    equity = np.zeros(n)
    cur_k = 0
    trades = 0
    trade_log = []

    def pv(i):
        return shares * close[i] + cash

    # 找到首个有效 MA
    valid0 = np.where(~np.isnan(ma))[0]
    if len(valid0) == 0:
        return equity, 0, trade_log
    start_i = valid0[0]
    # 初始化为中点仓位
    px = close[start_i]
    shares = INIT_CAPITAL * (BASE_SHARES * EACH) / px
    cash = INIT_CAPITAL - shares * px
    cur_k = 0

    for i in range(start_i, n):
        m = ma[i]
        if np.isnan(m) or m <= 0:
            equity[i] = pv(i)
            continue
        d = close[i] / m - 1.0
        k = int(round(d / BAND))
        if k != cur_k:
            target_shares = int(np.clip(BASE_SHARES - k, 0, TOTAL_SHARES))
            target_pos = target_shares * EACH
            value = pv(i)
            px = close[i]
            # 清旧仓
            if shares > 0:
                cash += shares * px * (1 - FEE)
                shares = 0
            # 建目标仓
            target_amt = value * target_pos
            if target_amt > 0 and px > 0:
                shares = target_amt / px
                cash = value - target_amt
            trades += 1
            trade_log.append({
                "date": str(date[i])[:10], "k": k,
                "pos": target_pos * 100, "action": "SELL" if k > cur_k else "BUY",
            })
            cur_k = k
        equity[i] = 0  # placeholder
        equity[i] = pv(i)

    equity[:start_i] = INIT_CAPITAL
    return equity, trades, trade_log


def calc_metrics(equity):
    total_ret = (equity[-1] / INIT_CAPITAL - 1) * 100
    peak = np.maximum.accumulate(equity)
    dd = (peak - equity) / peak * 100
    max_dd = dd.max()
    years = len(equity) / 252
    cagr = ((equity[-1] / INIT_CAPITAL) ** (1 / years) - 1) * 100 if years > 0 else 0
    dr = equity[1:] / equity[:-1] - 1
    dr = dr[np.isfinite(dr)]
    sharpe = dr.mean() / dr.std() * np.sqrt(252) if dr.std() > 0 else 0
    calmar = cagr / max_dd if max_dd > 0 else 0
    return {"total_return": total_ret, "max_drawdown": max_dd,
            "cagr": cagr, "sharpe": sharpe, "calmar": calmar}


def main():
    sql = """
        SELECT code, date, open, close, high, low, volume
        FROM fund_etf_hist_em
        WHERE date >= '2018-01-01' AND date <= '2026-07-21'
        AND code = '513300'
        ORDER BY date
    """
    raw = pd.read_sql(sql, con=mdb.engine())
    data = raw[raw['code'] == '513300'].copy()

    # 买入持有
    d2 = data.copy(); d2['_d'] = pd.to_datetime(d2['date'])
    period = d2[(d2['_d'] >= pd.Timestamp(START)) & (d2['_d'] <= pd.Timestamp(END))].sort_values('date')
    bp = period['close'].astype(float).values
    bh_return = (bp[-1] / bp[0] - 1) * 100
    bh_dd = ((np.maximum.accumulate(bp) - bp) / np.maximum.accumulate(bp) * 100).max()

    print()
    print("=" * 86)
    print("  纳指ETF(513300) 20份×5% 围绕MAn 7%网格  (2022全年熊市, 含万3手续费)")
    print("  规则: 价格=MA→50%仓; 每高7%卖1份(5%); 每低7%买1份(5%); 档位变才调仓")
    print("=" * 86)
    print()
    print(f"  {'锚定均线':<10s} {'收益':>10s} {'最大回撤':>10s} {'年化':>8s} {'Sharpe':>8s} {'Calmar':>8s} {'交易次数':>8s}")
    print(f"  {'─' * 76}")
    print(f"  {'买入持有':<10s} {bh_return:>+9.1f}% {bh_dd:>9.1f}% {'—':>8s} {'—':>8s} {'—':>8s} {0:>8d}")

    results = {}
    for w in MA_ANCHORS:
        eq, trades, log = backtest_ma_grid(data, w)
        m = calc_metrics(eq)
        results[w] = m
        print(f"  {'MA' + str(w):<10s} {m['total_return']:>+9.1f}% {m['max_drawdown']:>9.1f}% "
              f"{m['cagr']:>7.1f}% {m['sharpe']:>8.2f} {m['calmar']:>8.2f} {trades:>8d}")

    print()
    best = max(results, key=lambda w: results[w]['total_return'])
    print(f"  收益最高: MA{best} ({results[best]['total_return']:+.1f}%)")
    print()
    print("  对照(前一轮单均线趋势跟踪, 二进制满仓/空仓):")
    print("    MA20 +55% / MA30 +76% / MA60 +63% / MA120 +63%   买入持有 +147%")
    print()


if __name__ == '__main__':
    main()
