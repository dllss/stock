"""
纳指ETF(513300) 波谷(摆动低点)检测

思路：纳指是"波浪向上"走势，每一段回调的最低点就是波谷。
我们用 scipy.signal.find_peaks 在 -close 上找局部极小，
再用"相对前高回撤 >= MIN_DD"过滤掉噪音小波动，只保留真正的波谷。

输出：
  1. 每个波谷：日期、价格、相对前高的回撤深度、距上一波谷天数、之后到下一波峰的涨幅
  2. 一张标注波谷(绿)/波峰(红)的走势图
  3. 统计：波谷数量、平均回撤、波谷→波峰平均涨幅
"""

import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)

import instock.lib.database as mdb

try:
    from scipy.signal import find_peaks
    HAVE_SCIPY = True
except Exception:
    HAVE_SCIPY = False

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

INIT_CAPITAL = 1_000_000
START = '2021-01-01'
END = '2026-07-21'
MIN_DD = 0.08          # 至少回撤 8% 才算一个"波谷"
DIST = 8               # 相邻波谷至少间隔 8 个交易日
LOOKBACK = 60          # 计算"前高"的回看窗口(交易日)


def detect_swings(close, dates):
    """返回 (波谷索引列表, 波峰索引列表)"""
    if HAVE_SCIPY:
        trough_cand = find_peaks(-close, distance=DIST)[0]
        peak_cand = find_peaks(close, distance=DIST)[0]
    else:
        # 手动局部极小/极大
        trough_cand, peak_cand = [], []
        n = len(close)
        for i in range(DIST, n - DIST):
            w = close[max(0, i - DIST):i + DIST + 1]
            if close[i] == w.min():
                trough_cand.append(i)
            if close[i] == w.max():
                peak_cand.append(i)

    # 过滤：波谷需相对前高回撤 >= MIN_DD
    troughs = []
    for idx in trough_cand:
        start = max(0, idx - LOOKBACK)
        local_high = close[start:idx + 1].max()
        dd = close[idx] / local_high - 1
        if dd <= -MIN_DD:
            troughs.append(idx)

    # 波峰需相对前低上涨 >= MIN_DD
    peaks = []
    for idx in peak_cand:
        start = max(0, idx - LOOKBACK)
        local_low = close[start:idx + 1].min()
        up = close[idx] / local_low - 1
        if up >= MIN_DD:
            peaks.append(idx)
    return troughs, peaks


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
    mask = (data['_d'] >= pd.Timestamp(START)) & (data['_d'] <= pd.Timestamp(END))
    data = data[mask].sort_values('date').reset_index(drop=True)

    close = data['close'].astype(float).values
    dates = data['date'].values
    n = len(data)

    troughs, peaks = detect_swings(close, dates)

    # 合并排序所有拐点，配对成"波谷→下一波峰"
    allpts = sorted(set(troughs) | set(peaks))
    pts_kind = {}
    for i in troughs:
        pts_kind[i] = 'T'   # trough
    for i in peaks:
        pts_kind[i] = 'P'   # peak

    print()
    print("=" * 92)
    print("  纳指ETF(513300) 波谷(摆动低点)检测  2021.01~2026.07")
    print(f"  过滤条件: 相对前高回撤>={MIN_DD:.0%}, 相邻间隔>={DIST}日")
    print("=" * 92)
    print()
    print(f"  {'波谷日期':<12s} {'价格':>8s} {'回撤(对前高)':>14s} {'距上谷(天)':>12s} {'→下一波峰涨幅':>16s}")
    print(f"  {'─' * 78}")

    prev_trough_date = None
    t2p_gains = []
    dd_list = []
    for i, idx in enumerate(troughs):
        # 找该波谷之后的下一个波峰
        nxt_peak = None
        for p in peaks:
            if p > idx:
                nxt_peak = p
                break
        price = close[idx]
        # 前高
        start = max(0, idx - LOOKBACK)
        local_high = close[start:idx + 1].max()
        dd = price / local_high - 1
        dd_list.append(dd)
        # 距上一波谷天数
        gap = ''
        if prev_trough_date is not None:
            gap = str((pd.Timestamp(dates[idx]) - pd.Timestamp(prev_trough_date)).days)
        # 到下一波峰涨幅
        fwd = ''
        if nxt_peak is not None:
            gain = close[nxt_peak] / price - 1
            fwd = f"{gain:>+14.1%}"
            t2p_gains.append(gain)
        prev_trough_date = dates[idx]
        print(f"  {str(dates[idx])[:10]:<12s} {price:>8.3f} {dd:>13.1%} {gap:>12s} {fwd}")

    print()
    print(f"  波谷总数: {len(troughs)}")
    print(f"  平均回撤深度: {np.mean(dd_list):.1%}   (最深 {min(dd_list):.1%})")
    if t2p_gains:
        print(f"  波谷→下一波峰 平均涨幅: {np.mean(t2p_gains):+.1%}   (最大 {max(t2p_gains):+.1%})")
        win = sum(1 for g in t2p_gains if g > 0)
        print(f"  波谷后上涨胜率: {win}/{len(t2p_gains)} = {win/len(t2p_gains):.0%}")

    # 画图（用英文标签避免中文字体缺失）
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.plot(range(n), close, color='#444', lw=1.0, label='513300 close')
    if troughs:
        ax.scatter(troughs, close[troughs], color='green', s=45, zorder=5, label='Trough (buy)')
    if peaks:
        ax.scatter(peaks, close[peaks], color='red', s=45, zorder=5, label='Peak (sell)')
    ax.set_title('Nasdaq ETF (513300) Swing Trough/Peak Detection 2021-2026', fontsize=13)
    ax.set_xlabel('Trading day index')
    ax.set_ylabel('Price')
    ax.legend()
    ax.grid(alpha=0.3)
    out = 'script/plots/513300_troughs.png'
    fig.savefig(out, dpi=110, bbox_inches='tight')
    print()
    print(f"  走势图已保存: {out}")


if __name__ == '__main__':
    main()
