"""
V22.1 状态机 → A股ETF 精简3态版 (V2)

针对 V1 (6态) 的缺陷改进：
  V1 问题：纳斯达克 89 次切换，跌破MA200后在 -10%~-20% 反复拉锯
  
V2 核心改进 - "不可逆"防震荡约束：
  1. 只有 3 个大状态：BULL(满仓) / CAUTION(半仓) / PANIC_BUY(抄底)
  2. 一旦从 BULL 降到 CAUTION，不允许立刻回 BULL
     → 必须等价格回 MA200 上方 + MA20 向上 + 最少冷却 N 天
  3. PANIC_BUY（深度回撤）触发后，只加仓不卖，直到价格重上 MA200
  4. 去掉敏感的 TOP_ESCAPE 信号（它对单 ETF 有害无益）
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
# 策略参数
# ═══════════════════════════════════════
INIT_CAPITAL = 1_000_000
MA_LONG = 200
MA_SHORT = 20
MIN_HOLD_DAYS = 10      # 状态切换后最少保持 N 天，防止反复横跳
RECOVER_DD = -0.10      # 回撤恢复到 -10% 以上才算"企稳"
PANIC_DD = -0.20        # 回撤超过 -20% 进入抄底区

# 状态 → (ETF仓位%, 现金%)
STATE_WEIGHTS = {
    "BULL":      (0.90, 0.10),   # MA200上方 + 健康 → 90%满仓
    "CAUTION":   (0.50, 0.50),   # 跌破MA200但没跌透 → 半仓观望（最危险区）
    "PANIC_BUY": (1.00, 0.00),   # 深度回撤 > 20% → 满仓抄底（机会区）
}

# 状态切换方向：用于冷却逻辑
# BULL 是 risk_on; CAUTION/PANIC_BUY 是 risk_off
RISK_OFF = {"CAUTION", "PANIC_BUY"}


def calc_ma(arr, window, i):
    if i < window - 1:
        return np.nan
    return arr[max(0, i - window + 1):i + 1].mean()


def backtest_state_machine_v2(df, start_date, end_date):
    """V2 精简3态状态机回测"""
    data = df.copy()
    data['_d'] = pd.to_datetime(data['date'])
    mask = (data['_d'] >= pd.Timestamp(start_date)) & (data['_d'] <= pd.Timestamp(end_date))
    data = data[mask].sort_values('date').reset_index(drop=True)
    n = len(data)
    if n < MA_LONG + 10:
        return None, None, None

    close = data['close'].astype(float).values
    open_ = data['open'].astype(float).values
    ma200 = np.array([calc_ma(close, MA_LONG, i) for i in range(n)])
    ma20 = np.array([calc_ma(close, MA_SHORT, i) for i in range(n)])
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

    for i in range(MA_LONG + 5, n):
        c, o = close[i], open_[i]
        days_in_state += 1

        dd = (c / ath[i] - 1.0) if ath[i] > 0 else 0.0
        above_ma200 = (not np.isnan(ma200[i])) and (c > ma200[i])
        ma20_up = (not np.isnan(ma20[i])) and (i > 0) and (not np.isnan(ma20[i - 1])) and (ma20[i] > ma20[i - 1])

        # ── 状态机判断 ──
        next_state = state

        if state == "BULL":
            if not above_ma200:
                next_state = "CAUTION"   # 跌破MA200 → 半仓

        elif state == "CAUTION":
            if dd <= PANIC_DD:
                next_state = "PANIC_BUY"  # 深度回撤 → 抄底
            elif above_ma200 and ma20_up and days_in_state >= MIN_HOLD_DAYS:
                next_state = "BULL"       # 企稳回到MA200上方 + MA20向上 → 满仓

        elif state == "PANIC_BUY":
            # 抄底后只加不卖：直到价格重上 MA200 且回撤恢复 → 回到 BULL
            if above_ma200 and dd > RECOVER_DD and days_in_state >= MIN_HOLD_DAYS:
                next_state = "BULL"
            # 否则保持 PANIC_BUY（已经满仓，不加也不减）

        # ── 执行调仓（仅在状态变化时）──
        if next_state != state:
            prev = state
            state = next_state
            days_in_state = 0
            etf_pct, _ = STATE_WEIGHTS[state]
            value = pv(i)

            # 全部清仓旧头寸
            if shares > 0:
                cash += shares * c
                shares = 0

            # 买入新目标仓位
            target = value * etf_pct
            if target > 0 and c > 0:
                shares = target / c
                cash = value - target

            trades.append({
                "date": str(data['date'].iloc[i]),
                "from": prev,
                "to": state,
                "dd_pct": dd * 100,
                "etf_pct": etf_pct * 100,
                "value": value,
            })

        equity_curve[i] = pv(i)
        state_log[i] = state

    # 填充
    for i in range(MA_LONG + 5, n):
        if equity_curve[i] == 0:
            equity_curve[i] = equity_curve[i - 1]
    equity_curve[:MA_LONG + 5] = equity_curve[MA_LONG + 5]

    return equity_curve, trades, state_log


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
    return {
        "total_return": total_ret, "max_drawdown": max_dd,
        "cagr": cagr, "sharpe": sharpe, "calmar": calmar,
    }


# ═══════════════════════════════════════
# 主流程
# ═══════════════════════════════════════

sql = """
    SELECT code, date, open, close, high, low, volume
    FROM fund_etf_hist_em
    WHERE date >= '2019-01-01' AND date <= '2026-07-21'
    AND code IN ('513300','512890','513500')
    ORDER BY code, date
"""
raw = pd.read_sql(sql, con=mdb.engine())
ETF_NAMES = {'513300': '纳斯达克', '512890': '红利低波', '513500': '标普500'}

print()
print("=" * 80)
print("  V22.1 状态机 → A股ETF 精简3态版 (V2)")
print("  关键改进：不可逆冷却约束 + 去 TOP_ESCAPE 信号")
print("=" * 80)
print()

results = {}

for code in ['513300', '512890', '513500']:
    name = ETF_NAMES[code]
    data = raw[raw['code'] == code].copy()

    eq, trades, states = backtest_state_machine_v2(data, '2021-01-01', '2026-07-21')
    m = calc_metrics(eq)

    # 买入持有
    data['_d'] = pd.to_datetime(data['date'])
    mask = (data['_d'] >= pd.Timestamp('2021-01-01')) & (data['_d'] <= pd.Timestamp('2026-07-21'))
    period = data[mask].sort_values('date')
    bh_start = float(period.iloc[0]['close'])
    bh_end = float(period.iloc[-1]['close'])
    bh_return = (bh_end / bh_start - 1) * 100
    bh_prices = period['close'].astype(float).values
    bh_peak = np.maximum.accumulate(bh_prices)
    bh_dd = (bh_peak - bh_prices) / bh_peak * 100
    bh_max_dd = bh_dd.max()

    results[code] = {**m, "bh_return": bh_return, "bh_dd": bh_max_dd}

    state_days = pd.Series(states[states != '']).value_counts()
    total_days = state_days.sum()

    print(f"  {name} ({code})")
    print(f"  ├─ 策略收益: {m['total_return']:>+8.1f}%    买入持有: {bh_return:>+8.1f}%    差值: {m['total_return']-bh_return:>+7.1f}%")
    print(f"  ├─ 最大回撤: {m['max_drawdown']:>8.1f}%    BH回撤:   {bh_max_dd:>8.1f}%    改善: {bh_max_dd-m['max_drawdown']:>+7.1f}%")
    print(f"  ├─ 年化/Sharpe/Calmar: {m['cagr']:.1f}% / {m['sharpe']:.2f} / {m['calmar']:.2f}")
    print(f"  ├─ 状态切换: {len(trades)} 次")
    print(f"  ├─ 状态分布 ({total_days}交易日):", end="")
    for st, cnt in state_days.items():
        print(f"  {st}={cnt/total_days*100:.0f}%", end="")
    print()
    print(f"  └─ 切换明细:")
    for t in trades:
        print(f"       {t['date']}  {t['from']:>10s}→{t['to']:<10s} 回撤{t['dd_pct']:+7.1f}% →仓位{t['etf_pct']:.0f}%")
    print()

# 等权组合
print("=" * 80)
print("  三 ETF 等权组合")
print("=" * 80)
strat_avg = np.mean([results[c]['total_return'] for c in ETF_NAMES])
bh_avg = np.mean([results[c]['bh_return'] for c in ETF_NAMES])
dd_avg = np.mean([results[c]['max_drawdown'] for c in ETF_NAMES])
bh_dd_avg = np.mean([results[c]['bh_dd'] for c in ETF_NAMES])
sharpe_avg = np.mean([results[c]['sharpe'] for c in ETF_NAMES])

print(f"\n  {'指标':<12s} {'V2状态机':>12s} {'买入持有':>12s} {'差值':>12s}")
print(f"  {'─'*50}")
print(f"  {'总收益':<12s} {strat_avg:>+11.1f}% {bh_avg:>+11.1f}% {strat_avg-bh_avg:>+11.1f}%")
print(f"  {'最大回撤':<12s} {dd_avg:>11.1f}% {bh_dd_avg:>11.1f}% {dd_avg-bh_dd_avg:>+11.1f}%")
print(f"  {'Sharpe':<12s} {sharpe_avg:>11.2f}")
print()
print("  对比 V1 (6态) 等权: 总收益 +62.4%  回撤 16.7%  89次切换(纳斯达克)")
print(f"  本版 V2 (3态) 等权: 总收益 {strat_avg:+.1f}%  回撤 {dd_avg:.1f}%  切换数见上")
print()
