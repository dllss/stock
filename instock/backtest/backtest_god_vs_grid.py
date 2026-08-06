"""
纳指ETF(513300) "上帝视角(完美抄底逃顶)" vs "20份网格(可行)" vs "买入持有" 资金曲线对比

上帝视角(不可达上界)：事后已知所有波谷/波峰，在波谷最低点满仓、波峰最高点空仓，
吃满每一段上涨、躲过每一段下跌。这是择时的理论上界。

20份网格(可行方案)：之前验证的 20份×5% 围绕MA30 的 7% 网格，档位变才调仓，含万3手续费。

买入持有：基准。

输出：一张三条资金曲线叠图 + 波谷(绿)/波峰(红)标记 + 指标对比表。
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
    HAVE_SCIPER = True
except Exception:
    HAVE_SCIPER = False
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

INIT_CAPITAL = 1_000_000
START = '2021-01-01'
END = '2026-07-21'
FEE = 0.0003
BAND = 0.07
TOTAL = 20
EACH = 1.0 / TOTAL
BASE = TOTAL // 2
MIN_DD = 0.08          # 波谷：相对前高回撤 >= 8%
DIST = 8               # 相邻拐点间隔
LOOKBACK = 60
MA_WIN = 30


def calc_ma(a, w):
    return pd.Series(a).rolling(w).mean().values


def detect_swings(close):
    """返回 (波谷索引, 波峰索引)"""
    if HAVE_SCIPER:
        tc = find_peaks(-close, distance=DIST)[0]
        pc = find_peaks(close, distance=DIST)[0]
    else:
        tc, pc = [], []
        n = len(close)
        for i in range(DIST, n - DIST):
            w = close[max(0, i - DIST):i + DIST + 1]
            if close[i] == w.min():
                tc.append(i)
            if close[i] == w.max():
                pc.append(i)
    troughs = [i for i in tc if close[i] / close[max(0, i - LOOKBACK):i + 1].max() - 1 <= -MIN_DD]
    peaks = [i for i in pc if close[i] / close[max(0, i - LOOKBACK):i + 1].min() - 1 >= MIN_DD]
    return troughs, peaks


def god_view(close, troughs, peaks):
    """完美抄底逃顶：上涨段[T,P)满仓，下跌段[P,T)空仓。不可达上界，无手续费。"""
    n = len(close)
    kind = {}
    for t in troughs:
        kind[t] = 1
    for p in peaks:
        kind[p] = 0
    spts = sorted(kind.keys())
    daily_pos = np.ones(n)   # 首个拐点前默认满仓
    for i in range(n):
        seg = None
        for sp in spts:
            if sp <= i:
                seg = sp
            else:
                break
        if seg is not None:
            daily_pos[i] = kind[seg]
    equity = np.zeros(n)
    equity[0] = INIT_CAPITAL
    ret = close[1:] / close[:-1] - 1
    for i in range(1, n):
        equity[i] = equity[i - 1] * (1 + daily_pos[i - 1] * ret[i - 1])
    return equity


def grid_backtest(close, ma):
    """20份×5% 围绕MA30 的7%网格，档位变才调仓，含万3手续费。"""
    n = len(close)
    equity = np.zeros(n)
    valid0 = np.where(~np.isnan(ma))[0]
    if len(valid0) == 0:
        return equity
    start_i = valid0[0]
    px = close[start_i]
    shares = INIT_CAPITAL * (BASE * EACH) / px
    cash = INIT_CAPITAL - shares * px
    cur_k = 0

    def pv(i):
        return shares * close[i] + cash

    for i in range(start_i, n):
        m = ma[i]
        if np.isnan(m) or m <= 0:
            equity[i] = pv(i)
            continue
        d = close[i] / m - 1.0
        k = int(round(d / BAND))
        if k != cur_k:
            ts = int(np.clip(BASE - k, 0, TOTAL))
            pos = ts * EACH
            val = pv(i)
            p = close[i]
            if shares > 0:
                cash += shares * p * (1 - FEE)
                shares = 0
            amt = val * pos
            if amt > 0 and p > 0:
                shares = amt / p
                cash = val - amt
            cur_k = k
        equity[i] = pv(i)
    equity[:start_i] = INIT_CAPITAL
    return equity


def metrics(eq):
    tr = (eq[-1] / INIT_CAPITAL - 1) * 100
    peak = peak = np.maximum.accumulate(eq)
    mdd = ((peak - eq) / peak * 100).max()
    years = len(eq) / 252
    cagr = ((eq[-1] / INIT_CAPITAL) ** (1 / years) - 1) * 100 if years > 0 else 0
    dr = eq[1:] / eq[:-1] - 1
    sharpe = dr.mean() / dr.std() * np.sqrt(252) if dr.std() > 0 else 0
    return tr, mdd, cagr, sharpe


# ── V2 状态机（精简3态，复制自 backtest_state_machine_v2.py，去顶层主流程，无手续费）
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
            trades.append({"date": str(data['date'].iloc[i]), "from": prev, "to": state})
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
    mask = (data['_d'] >= pd.Timestamp(START)) & (data['_d'] <= pd.Timestamp(END))
    period = data[mask].sort_values('date').reset_index(drop=True)
    close = period['close'].astype(float).values
    n = len(period)

    # MA30 需要全量数据预热，再截取 period 区间
    full = data.sort_values('date').reset_index(drop=True)
    full_close = full['close'].astype(float).values
    ma_full = calc_ma(full_close, MA_WIN)
    idx0 = int(np.argmax(full['_d'].values >= pd.Timestamp(START)))
    ma_period = ma_full[idx0:idx0 + n]

    troughs, peaks = detect_swings(close)
    eq_god = god_view(close, troughs, peaks)
    eq_grid = grid_backtest(close, ma_period)
    eq_bh = INIT_CAPITAL * close / close[0]
    eq_v2, _, _ = v2_backtest(data, START, END)
    assert len(eq_v2) == n, f"V2 len {len(eq_v2)} != n {n}"

    g_tr, g_mdd, g_cagr, g_sh = metrics(eq_god)
    r_tr, r_mdd, r_cagr, r_sh = metrics(eq_grid)
    b_tr, b_mdd, b_cagr, b_sh = metrics(eq_bh)
    v_tr, v_mdd, v_cagr, v_sh = metrics(eq_v2)

    print()
    print("=" * 86)
    print("  纳指ETF(513300) 资金曲线对比  2021.01~2026.07")
    print("  GodView=完美抄底逃顶(上界,无费) | V2=状态机(无费) | Grid=20份MA30 7%(含万3) | Buy&Hold")
    print("=" * 86)
    print()
    print(f"  {'策略':<26s} {'收益':>10s} {'最大回撤':>10s} {'年化':>8s} {'Sharpe':>8s}")
    print(f"  {'─' * 76}")
    print(f"  {'上帝视角(完美抄底逃顶)':<26s} {g_tr:>+9.1f}% {g_mdd:>9.1f}% {g_cagr:>7.1f}% {g_sh:>8.2f}")
    print(f"  {'V2状态机(3态)':<26s} {v_tr:>+9.1f}% {v_mdd:>9.1f}% {v_cagr:>7.1f}% {v_sh:>8.2f}")
    print(f"  {'20份网格(MA30,7%)':<26s} {r_tr:>+9.1f}% {r_mdd:>9.1f}% {r_cagr:>7.1f}% {r_sh:>8.2f}")
    print(f"  {'买入持有':<26s} {b_tr:>+9.1f}% {b_mdd:>9.1f}% {b_cagr:>7.1f}% {b_sh:>8.2f}")
    print()
    print(f"  V2 vs 上帝视角 收益差: {v_tr - g_tr:+.1f}%")
    print(f"  V2 vs 买入持有 收益差: {v_tr - b_tr:+.1f}%   (V2少赚但回撤更低)")
    print(f"  网格 vs 买入持有 收益差: {r_tr - b_tr:+.1f}%")
    print(f"  波谷数={len(troughs)}  波峰数={len(peaks)}")

    # 画图
    fig, ax = plt.subplots(figsize=(15, 6.5))
    x = range(n)
    ax.plot(x, eq_god, color='#1f77b4', lw=2.0, label=f'God-View (perfect) {g_tr:+.0f}%')
    ax.plot(x, eq_v2, color='#9467bd', lw=1.8, label=f'V2 State-Machine {v_tr:+.0f}%')
    ax.plot(x, eq_grid, color='#ff7f0e', lw=1.8, label=f'20-share Grid MA30 7% {r_tr:+.0f}%')
    ax.plot(x, eq_bh, color='#444444', lw=1.2, ls='--', label=f'Buy & Hold {b_tr:+.0f}%')
    if troughs:
        ax.scatter(troughs, eq_god[troughs], color='green', s=40, zorder=5, label='Trough (buy)')
    if peaks:
        ax.scatter(peaks, eq_god[peaks], color='red', s=40, zorder=5, label='Peak (sell)')
    ax.set_title('Nasdaq ETF (513300) God-View vs V2 vs Grid vs Buy&Hold 2021-2026', fontsize=13)
    ax.set_xlabel('Trading day index')
    ax.set_ylabel('Equity (CNY)')
    ax.legend(loc='upper left')
    ax.grid(alpha=0.3)
    out = 'script/plots/513300_god_vs_grid.png'
    fig.savefig(out, dpi=110, bbox_inches='tight')
    print()
    print(f"  对比图已保存: {out}")


if __name__ == '__main__':
    main()
