"""
V22.1 状态机策略 → A股ETF 适配版

核心思想（源自 QQQ/TQQQ V22.1）：
  - 不是问"买还是卖"，而是问"现在市场处于什么状态"
  - 用 ATH回撤深度 + MA200上下 + MA20方向 → 6种状态 × 6套仓位
  - 反直觉：跌得越深仓位越重；刚跌破MA200就空仓

对比：原始 V22.1 有 QQQ+TQQQ 两个标的，这里适配为单 ETF + 现金
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
VOL_WINDOW = 60
VOL_FACTOR = 2.0        # 逃顶信号：成交量 > 60日均量 × 2.0
HIGH_ZONE = 0.95        # 逃顶信号：价格 ≥ ATH × 0.95
MIN_RISK_OFF_DAYS = 2   # 防V反：至少等2天才能从防御转进攻

# 状态 → (ETF仓位%, 现金%)
STATE_WEIGHTS = {
    "NORMAL":             (0.90, 0.10),   # MA200上方，接近ATH，90%仓位
    "TOP_ESCAPE":         (0.50, 0.50),   # 接近ATH+放量阴线，减仓避险
    "BEAR_CASH":          (0.00, 1.00),   # 刚跌破MA200，跌得还不够多→最危险
    "ZONE_BATTLE_ATTACK": (1.00, 0.00),   # 跌破MA200跌了10-30%，MA20向上→抄底
    "ZONE_BATTLE_DEFEND": (0.50, 0.50),   # 跌破MA200跌了10-30%，MA20向下→观望
    "ZONE_DESPAIR":       (1.00, 0.00),   # 跌超30%→绝望区，满仓抄底
}
RISK_OFF = {"BEAR_CASH", "ZONE_BATTLE_DEFEND", "TOP_ESCAPE"}
RISK_ON = {"ZONE_BATTLE_ATTACK", "NORMAL"}


def calc_ma(arr, window, i):
    """计算第 i 点的 window 日均线"""
    if i < window - 1:
        return np.nan
    return arr[max(0, i - window + 1):i + 1].mean()


def backtest_state_machine(df, start_date, end_date, name=""):
    """
    对单个 ETF 运行 V22.1 风格的状态机回测

    参数:
        df: 含 date/open/high/low/close/volume 列的 DataFrame
        start_date, end_date: 回测区间
        name: ETF 名称（用于打印）
    """
    # 对齐日期
    mask = (df['date'].astype(str) >= str(start_date)) & (df['date'].astype(str) <= str(end_date))
    df = df[mask].sort_values('date').reset_index(drop=True)
    n = len(df)
    if n < MA_LONG + 10:
        return None, None, None

    close = df['close'].astype(float).values
    open_ = df['open'].astype(float).values
    vol = df['volume'].astype(float).values

    # 预计算所有指标
    ma200 = np.array([calc_ma(close, MA_LONG, i) for i in range(n)])
    ma20 = np.array([calc_ma(close, MA_SHORT, i) for i in range(n)])
    vol_ma60 = np.array([calc_ma(vol, VOL_WINDOW, i) for i in range(n)])
    ath = np.array([close[max(0, i - 252):i + 1].max() for i in range(n)])

    # 状态变量
    state = "NORMAL"
    shares = 0.0
    cash = float(INIT_CAPITAL)
    risk_off_days = 0
    trades = []              # 交易记录
    equity_curve = np.zeros(n)
    state_log = np.full(n, "", dtype=object)

    def portfolio_value(i):
        return shares * close[i] + cash

    # 从第 MA_LONG+5 天开始（等所有指标就绪）
    for i in range(MA_LONG + 5, n):
        c, o, v = close[i], open_[i], vol[i]

        # 更新 risk_off 计数器
        risk_off_days = risk_off_days + 1 if state in RISK_OFF else 0

        # ── 状态机判断 ──
        dd = (c / ath[i] - 1.0) if ath[i] > 0 else 0.0

        # 逃顶信号
        is_top = False
        if ath[i] > 0 and c >= ath[i] * HIGH_ZONE:
            if vol_ma60[i] > 0 and v > vol_ma60[i] * VOL_FACTOR:
                if c < o:  # 放量阴线
                    is_top = True

        # 原始状态
        if is_top:
            raw_next = "TOP_ESCAPE"
        elif not np.isnan(ma200[i]) and c < ma200[i]:
            if dd <= -0.30:
                raw_next = "ZONE_DESPAIR"
            elif dd <= -0.10:
                raw_next = "ZONE_BATTLE_ATTACK" if (not np.isnan(ma20[i]) and c > ma20[i]) else "ZONE_BATTLE_DEFEND"
            else:
                raw_next = "BEAR_CASH"
        else:
            raw_next = "ZONE_BATTLE_ATTACK" if dd < -0.10 else "NORMAL"

        # Anti-V 反转过滤
        next_state = raw_next
        if state in RISK_OFF and raw_next in RISK_ON:
            blocked = False
            if risk_off_days < MIN_RISK_OFF_DAYS:
                blocked = True
            if not (not np.isnan(ma20[i]) and i > 0 and not np.isnan(ma20[i - 1]) and ma20[i] > ma20[i - 1]):
                blocked = True
            if blocked:
                next_state = state

        # ── 执行调仓 ──
        if next_state != state:
            prev = state
            state = next_state
            etf_pct, cash_pct = STATE_WEIGHTS[state]
            pv = portfolio_value(i)

            # 全部卖出旧仓位（简化：以今日收盘价计价）
            old_shares = shares
            if old_shares > 0:
                cash += old_shares * c
                shares = 0

            # 买入新目标仓位
            target_value = pv * etf_pct
            if target_value > 0 and c > 0:
                shares = target_value / c
                cash = pv - target_value

            trades.append({
                "date": str(df['date'].iloc[i]),
                "from": prev,
                "to": state,
                "dd_pct": f"{dd*100:.1f}%",
                "etf_pct": f"{etf_pct*100:.0f}%",
                "value": pv,
            })

        # 记录净值
        equity_curve[i] = portfolio_value(i)
        state_log[i] = state

    # 填充未计算的天
    for i in range(MA_LONG + 5, n):
        if equity_curve[i] == 0:
            equity_curve[i] = equity_curve[i - 1]
    equity_curve[:MA_LONG + 5] = equity_curve[MA_LONG + 5]

    return equity_curve, trades, state_log


def calc_metrics(equity):
    """计算收益/回撤/年化/Sharpe"""
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
        "total_return": total_ret,
        "max_drawdown": max_dd,
        "cagr": cagr,
        "sharpe": sharpe,
        "calmar": calmar,
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
print("=" * 95)
print("  V22.1 状态机策略 → A股ETF 适配版 回测报告")
print("  思想来源: QQQ/TQQQ 双核量化 V22.1 (财富种植园)")
print("=" * 95)
print()

all_metrics = {}

for code in ['513300', '512890', '513500']:
    name = ETF_NAMES[code]
    data = raw[raw['code'] == code].copy()

    # 状态机策略
    eq, trades, states = backtest_state_machine(data, '2021-01-01', '2026-07-21', name)
    m = calc_metrics(eq)

    # 买入持有
    data['_d'] = pd.to_datetime(data['date'])
    mask = (data['_d'] >= pd.Timestamp('2021-01-01')) & (data['_d'] <= pd.Timestamp('2026-07-21'))
    period = data[mask].sort_values('date')
    bh_start = float(period.iloc[0]['close'])
    bh_end = float(period.iloc[-1]['close'])
    bh_return = (bh_end / bh_start - 1) * 100

    # 回撤（买入持有）
    bh_prices = period['close'].astype(float).values
    bh_peak = np.maximum.accumulate(bh_prices)
    bh_dd = (bh_peak - bh_prices) / bh_peak * 100
    bh_max_dd = bh_dd.max()

    all_metrics[code] = {**m, "bh_return": bh_return, "bh_dd": bh_max_dd}

    # 统计状态分布
    state_days = pd.Series(states[states != '']).value_counts()
    total_days = state_days.sum()

    print(f"  ┌{'─' * 85}┐")
    print(f"  │  {name} ({code})")
    print(f"  ├{'─' * 85}┤")
    print(f"  │  策略收益: {m['total_return']:>+10.1f}%    买入持有: {bh_return:>+10.1f}%    差值: {m['total_return']-bh_return:>+7.1f}%")
    print(f"  │  最大回撤: {m['max_drawdown']:>10.1f}%    BH最大回撤: {bh_max_dd:>10.1f}%    改善: {bh_max_dd-m['max_drawdown']:>+7.1f}%")
    print(f"  │  年化收益: {m['cagr']:>10.1f}%    Sharpe: {m['sharpe']:>7.2f}    Calmar: {m['calmar']:>7.2f}")
    print(f"  │  交易次数: {len(trades):>5}次")
    print(f"  ├{'─' * 85}┤")

    # 状态切换明细
    print(f"  │  状态切换记录:")
    for t in trades:
        print(f"  │    {t['date']}  {t['from']:>22s} → {t['to']:<22s}  回撤{t['dd_pct']:>7s}  →仓位{t['etf_pct']}")

    print(f"  ├{'─' * 85}┤")
    print(f"  │  状态分布 ({total_days}个交易日):")
    for st, cnt in state_days.items():
        bar = "█" * int(cnt / total_days * 40)
        pct = cnt / total_days * 100
        label = st[:22]
        print(f"  │    {label:<22s} {cnt:>4d}天 ({pct:5.1f}%)  {bar}")

    print(f"  └{'─' * 85}┘")
    print()

# ── 三ETF等权组合 ──
print("=" * 95)
print("  三 ETF 等权组合对比")
print("=" * 95)
print()
print(f"  {'指标':<20s} {'V22.1状态机':>14s} {'买入持有':>14s} {'差值':>14s}")
print(f"  {'-' * 65}")

# 等权收益
strat_avg = np.mean([all_metrics[c]['total_return'] for c in ETF_NAMES])
bh_avg = np.mean([all_metrics[c]['bh_return'] for c in ETF_NAMES])
dd_strat_avg = np.mean([all_metrics[c]['max_drawdown'] for c in ETF_NAMES])
dd_bh_avg = np.mean([all_metrics[c]['bh_dd'] for c in ETF_NAMES])
sharpe_avg = np.mean([all_metrics[c]['sharpe'] for c in ETF_NAMES])

print(f"  {'总收益':<20s} {strat_avg:>+13.1f}% {bh_avg:>+13.1f}% {strat_avg-bh_avg:>+13.1f}%")
print(f"  {'最大回撤':<20s} {dd_strat_avg:>13.1f}% {dd_bh_avg:>13.1f}% {dd_strat_avg-dd_bh_avg:>+13.1f}%")
print(f"  {'年化收益':<20s} {np.mean([all_metrics[c]['cagr'] for c in ETF_NAMES]):>13.1f}%")
print(f"  {'Sharpe':<20s} {sharpe_avg:>13.2f}")
print()
print("  💡 关键发现：")
print(f"     1. 状态机策略基于\"回撤深度\"而非\"趋势方向\"来做仓位决策")
print(f"     2. 核心创新：刚跌破MA200但没跌多少 → 全空仓（BEAR_CASH）")
print(f"     3. 跌得越深越敢买：-10%~-30% → 进攻，>30% → 绝望抄底")
print(f"     4. Anti-V反转过滤器防止震荡市反复被割")
print()
