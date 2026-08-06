"""
纳指ETF(513300) 单均线趋势跟踪 - MA周期扫描

规则（经典趋势跟踪）：
  收盘价 > MAn  → 满仓持有
  收盘价 < MAn  → 空仓（持币）
  次日开盘执行（避免用当日收盘信号成交的未来函数）

扫描周期：MA5 / MA10 / MA20 / MA30 / MA60 / MA120
对比：买入持有
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb

INIT_CAPITAL = 1_000_000
START = '2021-01-01'
END = '2026-07-21'
FEE = 0.0003          # 单边万3手续费
MA_LIST = [5, 10, 20, 30, 60, 120]


def calc_ma(arr, window):
    s = pd.Series(arr)
    return s.rolling(window).mean().values


def backtest_ma_trend(df, ma_win, warmup=120):
    """价格>MA满仓, <MA空仓, 次日开盘成交, 计入手续费"""
    data = df.copy()
    data['_d'] = pd.to_datetime(data['date'])
    # 多取 warmup 天用于MA预热
    mask = (data['_d'] >= pd.Timestamp(START)) & (data['_d'] <= pd.Timestamp(END))
    full = data.sort_values('date').reset_index(drop=True)
    close_all = full['close'].astype(float).values
    ma_all = calc_ma(close_all, ma_win)
    full['ma'] = ma_all
    period = full[mask].reset_index(drop=True)

    n = len(period)
    close = period['close'].astype(float).values
    open_ = period['open'].astype(float).values
    ma = period['ma'].values

    shares = 0.0
    cash = float(INIT_CAPITAL)
    equity = np.zeros(n)
    signal_prev = 0  # 上一日信号
    trades = 0

    for i in range(n):
        # 用 i-1 日信号在 i 日开盘执行
        if i > 0 and not np.isnan(ma[i - 1]):
            sig = 1 if close[i - 1] > ma[i - 1] else 0
            if sig != signal_prev:
                px = open_[i]
                if sig == 1:  # 买入
                    invest = cash
                    shares = invest * (1 - FEE) / px
                    cash = 0.0
                else:  # 卖出
                    cash = shares * px * (1 - FEE)
                    shares = 0.0
                trades += 1
                signal_prev = sig
        equity[i] = shares * close[i] + cash

    return equity, trades


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
        WHERE date >= '2019-01-01' AND date <= '2026-07-21'
        AND code = '513300'
        ORDER BY date
    """
    raw = pd.read_sql(sql, con=mdb.engine())
    data = raw[raw['code'] == '513300'].copy()

    # 买入持有基准
    d2 = data.copy()
    d2['_d'] = pd.to_datetime(d2['date'])
    period = d2[(d2['_d'] >= pd.Timestamp(START)) & (d2['_d'] <= pd.Timestamp(END))].sort_values('date')
    bh_prices = period['close'].astype(float).values
    bh_return = (bh_prices[-1] / bh_prices[0] - 1) * 100
    bh_peak = np.maximum.accumulate(bh_prices)
    bh_dd = ((bh_peak - bh_prices) / bh_peak * 100).max()

    print()
    print("=" * 84)
    print("  纳指ETF(513300) 单均线趋势跟踪 - MA周期扫描  (2021.01~2026.07, 含万3手续费)")
    print("  规则: 收盘价>MAn次日开盘满仓 / 收盘价<MAn次日开盘空仓")
    print("=" * 84)
    print()
    print(f"  {'策略':<14s} {'收益':>10s} {'最大回撤':>10s} {'年化':>8s} {'Sharpe':>8s} {'Calmar':>8s} {'交易次数':>8s}")
    print(f"  {'─' * 74}")
    print(f"  {'买入持有':<14s} {bh_return:>+9.1f}% {bh_dd:>9.1f}% {'—':>8s} {'—':>8s} {'—':>8s} {0:>8d}")

    results = {}
    for w in MA_LIST:
        eq, trades = backtest_ma_trend(data, w)
        m = calc_metrics(eq)
        results[w] = m
        print(f"  {'MA' + str(w):<14s} {m['total_return']:>+9.1f}% {m['max_drawdown']:>9.1f}% "
              f"{m['cagr']:>7.1f}% {m['sharpe']:>8.2f} {m['calmar']:>8.2f} {trades:>8d}")

    print()
    # 找最优
    best_ret = max(results, key=lambda w: results[w]['total_return'])
    best_sharpe = max(results, key=lambda w: results[w]['sharpe'])
    best_calmar = max(results, key=lambda w: results[w]['calmar'])
    print(f"  收益最高: MA{best_ret} ({results[best_ret]['total_return']:+.1f}%)")
    print(f"  Sharpe最高: MA{best_sharpe} ({results[best_sharpe]['sharpe']:.2f})")
    print(f"  Calmar最高: MA{best_calmar} ({results[best_calmar]['calmar']:.2f})")
    print()


if __name__ == '__main__':
    main()
