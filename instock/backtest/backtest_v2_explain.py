"""
纳指ETF(513300) V2 状态机(3态) 机制可视化

三态：
  BULL      满仓90%  —— MA200上方且健康
  CAUTION   半仓50%  —— 跌破MA200但未跌透(最危险区，减仓观望)
  PANIC_BUY 满仓100% —— 深度回撤>20%，满仓抄底(只加不卖)

切换约束(不可逆冷却)：
  BULL → CAUTION：价格跌破MA200
  CAUTION → PANIC_BUY：回撤≤-20%
  CAUTION → BULL：回到MA200上方 + MA20向上 + 冷却≥10天
  PANIC_BUY → BULL：回到MA200上方 + 回撤恢复到>-10% + 冷却≥10天

输出：状态切换明细 + 一张"价格曲线+状态背景色"图。
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)
import instock.lib.database as mdb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

INIT_CAPITAL = 1_000_000
START = '2021-01-01'
END = '2026-07-21'
V2_MA_LONG = 200
V2_MA_SHORT = 20
V2_MIN_HOLD_DAYS = 10
V2_RECOVER_DD = -0.10
V2_PANIC_DD = -0.20
V2_STATE_WEIGHTS = {
    "BULL": (0.90, 0.10),
    "CAUTION": (0.50, 0.50),
    "PANIC_BUY": (1.00, 0.00),
}


def v2_backtest(df, start_date, end_date):
    data = df.copy()
    data['_d'] = pd.to_datetime(data['date'])
    mask = (data['_d'] >= pd.Timestamp(start_date)) & (data['_d'] <= pd.Timestamp(end_date))
    data = data[mask].sort_values('date').reset_index(drop=True)
    n = len(data)
    if n < V2_MA_LONG + 10:
        return None, None, None
    close = data['close'].astype(float).values
    ma200 = np.array([(np.nan if i < V2_MA_LONG - 1 else close[max(0, i - V2_MA_LONG + 1):i + 1].mean()) for i in range(n)])
    ma20 = np.array([(np.nan if i < V2_MA_SHORT - 1 else close[max(0, i - V2_MA_SHORT + 1):i + 1].mean()) for i in range(n)])
    ath = np.array([close[max(0, i - 252):i + 1].max() for i in range(n)])
    state = "BULL"
    shares = 0.0
    cash = float(INIT_CAPITAL)
    days_in_state = 0
    trades = []
    equity_curve = np.zeros(n)
    state_log = np.full(n, "", dtype=object)

    def pv(i):
        return shares * close[i] + cash

    for i in range(V2_MA_LONG + 5, n):
        c = close[i]
        days_in_state += 1
        dd = (c / ath[i] - 1.0) if ath[i] > 0 else 0.0
        above_ma200 = (not np.isnan(ma200[i])) and (c > ma200[i])
        ma20_up = (not np.isnan(ma20[i])) and (i > 0) and (not np.isnan(ma20[i - 1])) and (ma20[i] > ma20[i - 1])
        next_state = state
        if state == "BULL":
            if not above_ma200:
                next_state = "CAUTION"
        elif state == "CAUTION":
            if dd <= V2_PANIC_DD:
                next_state = "PANIC_BUY"
            elif above_ma200 and ma20_up and days_in_state >= V2_MIN_HOLD_DAYS:
                next_state = "BULL"
        elif state == "PANIC_BUY":
            if above_ma200 and dd > V2_RECOVER_DD and days_in_state >= V2_MIN_HOLD_DAYS:
                next_state = "BULL"
        if next_state != state:
            prev = state
            state = next_state
            days_in_state = 0
            etf_pct, _ = V2_STATE_WEIGHTS[state]
            value = pv(i)
            if shares > 0:
                cash += shares * c
                shares = 0
            target = value * etf_pct
            if target > 0 and c > 0:
                shares = target / c
                cash = value - target
            trades.append({"date": str(data['date'].iloc[i])[:10], "from": prev, "to": state, "dd": dd * 100, "pos": etf_pct * 100})
        equity_curve[i] = pv(i)
        state_log[i] = state
    for i in range(V2_MA_LONG + 5, n):
        if equity_curve[i] == 0:
            equity_curve[i] = equity_curve[i - 1]
    equity_curve[:V2_MA_LONG + 5] = equity_curve[V2_MA_LONG + 5]
    return equity_curve, trades, state_log


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
    data['_d'] = pd.to_datetime(data['date'])
    eq, trades, states = v2_backtest(data, START, END)
    period = data[(data['_d'] >= pd.Timestamp(START)) & (data['_d'] <= pd.Timestamp(END))].sort_values('date').reset_index(drop=True)
    close = period['close'].astype(float).values
    n = len(close)
    dates = period['date'].astype(str).str[:10].values

    print()
    print("=" * 90)
    print("  V2 状态机(3态) — 纳指ETF(513300) 2021.01~2026.07 状态切换明细")
    print("=" * 90)
    print()
    print(f"  {'日期':<12s} {'切换':<22s} {'当时回撤':>10s} {'→仓位':>8s}")
    print(f"  {'─' * 56}")
    for t in trades:
        print(f"  {t['date']:<12s} {t['from']:>10s}→{t['to']:<10s} {t['dd']:>+9.1f}% {t['pos']:>6.0f}%")

    # 状态分布
    import collections
    cnt = collections.Counter([s for s in states if s])
    total = sum(cnt.values())
    print()
    print("  状态分布:")
    for st, c in cnt.items():
        print(f"    {st:<10s} {c:>5d} 天  ({c/total*100:.0f}%)  仓位 {V2_STATE_WEIGHTS[st][0]*100:.0f}%")

    # 画图：价格 + 状态背景色
    fig, ax = plt.subplots(figsize=(15, 6.5))
    x = range(n)
    # 状态分段背景
    segs = []
    cur = states[0]
    s0 = 0
    for i in range(1, n):
        if states[i] != cur:
            segs.append((s0, i, cur))
            cur = states[i]
            s0 = i
    segs.append((s0, n - 1, cur))
    col = {"BULL": "#cfe8cf", "CAUTION": "#ffe0b3", "PANIC_BUY": "#f5c6c6"}
    for s, e, st in segs:
        ax.axvspan(s, e, color=col.get(st, "#eeeeee"), alpha=0.55, zorder=0)

    ax.plot(x, close, color="#333", lw=1.0, zorder=3, label='513300 close')
    # MA200
    full = data.sort_values('date')
    ma200 = pd.Series(full['close'].astype(float).values).rolling(V2_MA_LONG).mean().values
    idx0 = int(np.argmax(full['_d'].values >= pd.Timestamp(START)))
    ax.plot(range(n), ma200[idx0:idx0 + n], color="#1f77b4", lw=1.0, ls=':', zorder=2, label='MA200')

    # 切换点标记
    for t in trades:
        # 找该日期在 x 的索引
        di = int(np.where(dates == t['date'])[0][0])
        ax.scatter(di, close[di], color="#d62728", s=45, zorder=5)
        ax.annotate(t['to'], (di, close[di]), textcoords="offset points", xytext=(0, 8),
                    fontsize=7, color="#d62728", ha="center")

    from matplotlib.patches import Patch
    legend_el = [
        Patch(facecolor=col["BULL"], label='BULL 90%'),
        Patch(facecolor=col["CAUTION"], label='CAUTION 50%'),
        Patch(facecolor=col["PANIC_BUY"], label='PANIC_BUY 100%'),
    ]
    ax.legend(handles=legend_el, loc='upper left')
    ax.set_title('Nasdaq ETF (513300) V2 State-Machine (3 states) 2021-2026', fontsize=13)
    ax.set_xlabel('Trading day index')
    ax.set_ylabel('Price')
    ax.grid(alpha=0.3)
    out = 'script/plots/513300_v2_states.png'
    fig.savefig(out, dpi=110, bbox_inches='tight')
    print()
    print(f"  状态图已保存: {out}")


if __name__ == '__main__':
    main()
