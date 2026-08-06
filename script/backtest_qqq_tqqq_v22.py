"""
QQQ/TQQQ 双核量化策略 V22.1 回测
来源: us-stock/strategies/v22/qqq_tqqq_dual_core_v22.py
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date, timedelta

# ═══════════════════════════════════════
# 策略参数（与 V22.1 完全一致）
# ═══════════════════════════════════════
MA_LONG = 200
MA_SHORT = 20
VOL_WINDOW = 60
VOL_FACTOR = 2.0
HIGH_ZONE = 0.95
MIN_RISK_OFF_DAYS = 2

# 各状态的目标权重 {state: (QQQ%, TQQQ%, cash%)}
TARGET_WEIGHTS = {
    "NORMAL":               (0.45, 0.45, 0.10),
    "TOP_ESCAPE":           (0.90, 0.00, 0.10),
    "ZONE_BATTLE_ATTACK":   (0.00, 0.99, 0.01),
    "ZONE_BATTLE_DEFEND":   (0.90, 0.00, 0.10),
    "BEAR_CASH":            (0.00, 0.00, 1.00),
    "ZONE_DESPAIR_TQQQ":    (0.00, 0.99, 0.01),
}

RISK_OFF_STATES = {"BEAR_CASH", "ZONE_BATTLE_DEFEND", "TOP_ESCAPE"}
RISK_ON_STATES = {"ZONE_BATTLE_ATTACK", "NORMAL"}


def fetch_data(ticker, start="2015-01-01"):
    """下载 QQQ / TQQQ 历史日线"""
    end = date.today().isoformat()
    df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"无法下载 {ticker}")
    # 扁平化 MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df = df.rename(columns={"date": "date"} if "date" in df.columns else {"index": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date").reset_index(drop=True)
    return df


def run_backtest_v22(df_qqq: pd.DataFrame, df_tqqq: pd.DataFrame,
                     init_capital=100000.0, start_date=None, end_date=None):
    """V22.1 完整逻辑回测"""
    # 对齐日期
    merged = df_qqq[["date", "open", "high", "low", "close", "volume"]].copy()
    merged = merged.merge(
        df_tqqq[["date", "close"]].rename(columns={"close": "tqqq_close"}),
        on="date", how="inner"
    )
    if start_date:
        merged = merged[merged["date"] >= start_date]
    if end_date:
        merged = merged[merged["date"] <= end_date]
    merged = merged.reset_index(drop=True)

    n = len(merged)
    if n < MA_LONG + 10:
        raise ValueError("数据天数不足以运行策略")

    # 预计算指标
    close = merged["close"].astype(float).values
    open_ = merged["open"].astype(float).values
    vol = merged["volume"].astype(float).values
    tqqq_close = merged["tqqq_close"].astype(float).values
    dates = merged["date"].values

    ma200 = np.full(n, np.nan)
    ma20 = np.full(n, np.nan)
    ath = np.full(n, np.nan)
    vol_ma60 = np.full(n, np.nan)

    for i in range(n):
        if i >= MA_LONG - 1:
            ma200[i] = close[i - MA_LONG + 1:i + 1].mean()
        if i >= MA_SHORT - 1:
            ma20[i] = close[i - MA_SHORT + 1:i + 1].mean()
        if i >= VOL_WINDOW:
            vol_ma60[i] = vol[i - VOL_WINDOW + 1:i + 1].mean()

    # ATH 初始化：回看 252 天
    for i in range(n):
        start = max(0, i - 252)
        ath[i] = close[start:i + 1].max()

    # ── 状态变量 ──
    state = "NORMAL"
    qqq_shares = 0.0
    tqqq_shares = 0.0
    cash = float(init_capital)
    risk_off_days = 0
    pending_buy = False
    pending_tg_q = 0.0
    pending_tg_t = 0.0
    trades = []

    portfolio_log = np.zeros(n)  # 每日净值记录

    for i in range(MA_LONG + 5, n):  # 等所有指标都就绪
        c = close[i]
        o = open_[i]
        t_c = tqqq_close[i]
        v = vol[i]
        d = dates[i]

        # 更新 risk_off_days
        if state in RISK_OFF_STATES:
            risk_off_days += 1
        else:
            risk_off_days = 0

        # ═══ 处理 T+1 买入挂单 ═══
        if pending_buy:
            tg_q, tg_t = pending_tg_q, pending_tg_t
            equity = qqq_shares * c + tqqq_shares * t_c + cash
            if equity <= 0:
                pending_buy = False
                continue

            target_q_val = equity * tg_q
            target_t_val = equity * tg_t
            curr_q_val = qqq_shares * c
            curr_t_val = tqqq_shares * t_c

            # 只买不卖（买入执行阶段）
            need_q = max(0, target_q_val - curr_q_val)
            need_t = max(0, target_t_val - curr_t_val)

            if need_q > 500 and c > 0 and cash >= need_q:
                buy_shares = need_q / c
                qqq_shares += buy_shares
                cash -= need_q
            elif need_q > 500 and c > 0:
                buy_shares = cash / c
                qqq_shares += buy_shares
                cash = 0

            if need_t > 500 and t_c > 0 and cash >= need_t:
                buy_shares = need_t / t_c
                tqqq_shares += buy_shares
                cash -= need_t
            elif need_t > 500 and t_c > 0:
                buy_shares = cash / t_c
                tqqq_shares += buy_shares
                cash = 0

            equity_after = qqq_shares * c + tqqq_shares * t_c + cash
            trades.append({
                "date": d, "action": "T+1_BUY",
                "state": state,
                "target_qqq": f"{tg_q:.0%}",
                "target_tqqq": f"{tg_t:.0%}",
                "equity": equity_after,
            })
            pending_buy = False

        # ═══ 计算信号 ═══
        dd = (c / ath[i] - 1.0) if ath[i] > 0 else 0.0

        # TOP_ESCAPE 信号
        is_top_signal = False
        if ath[i] > 0 and c >= ath[i] * HIGH_ZONE:
            if vol_ma60[i] > 0 and v > vol_ma60[i] * VOL_FACTOR:
                if c < o:  # 放量阴线
                    is_top_signal = True

        # 状态机判断 raw next_state
        if is_top_signal:
            raw_next = "TOP_ESCAPE"
        elif not np.isnan(ma200[i]) and c < ma200[i]:
            if dd <= -0.30:
                raw_next = "ZONE_DESPAIR_TQQQ"
            elif dd <= -0.10:
                if not np.isnan(ma20[i]) and c > ma20[i]:
                    raw_next = "ZONE_BATTLE_ATTACK"
                else:
                    raw_next = "ZONE_BATTLE_DEFEND"
            else:
                raw_next = "BEAR_CASH"
        else:
            if dd < -0.10:
                raw_next = "ZONE_BATTLE_ATTACK"
            else:
                raw_next = "NORMAL"

        # Anti-V 反转过滤
        next_state = raw_next
        blocked = False
        if state in RISK_OFF_STATES and raw_next in RISK_ON_STATES:
            if risk_off_days < MIN_RISK_OFF_DAYS:
                blocked = True
            if not np.isnan(ma20[i]) and i > 0 and not np.isnan(ma20[i - 1]):
                if ma20[i] <= ma20[i - 1]:
                    blocked = True
            else:
                blocked = True  # 数据不足

        if blocked:
            next_state = state

        # ═══ NORMAL 状态再平衡 ═══
        need_rebalance = False
        if next_state == "NORMAL" and state == "NORMAL":
            equity = qqq_shares * c + tqqq_shares * t_c + cash
            if equity > 0:
                val_q = qqq_shares * c
                val_t = tqqq_shares * t_c
                invested = val_q + val_t
                if invested > 0:
                    deviation = abs(val_q - val_t) / invested
                    if deviation > 0.20:
                        need_rebalance = True

        # ═══ 状态切换 ──
        state_changed = (next_state != state) or need_rebalance
        if not state_changed:
            # 记录净值
            portfolio_log[i] = qqq_shares * c + tqqq_shares * t_c + cash
            continue

        prev_state = state
        state = next_state
        tg_q, tg_t, _ = TARGET_WEIGHTS[state]
        equity = qqq_shares * c + tqqq_shares * t_c + cash

        target_val_q = equity * tg_q
        target_val_t = equity * tg_t
        curr_val_q = qqq_shares * c
        curr_val_t = tqqq_shares * t_c
        diff_q = target_val_q - curr_val_q
        diff_t = target_val_t - curr_val_t

        sold = False

        # 卖出 QQQ
        if diff_q < -500 and c > 0:
            sell_val = min(abs(diff_q), curr_val_q)
            sell_shares = sell_val / c
            qqq_shares -= sell_shares
            cash += sell_val
            sold = True
            trades.append({
                "date": d, "action": "SELL_QQQ",
                "state_from": prev_state, "state_to": state,
                "amount": sell_val, "shares": sell_shares,
            })

        # 卖出 TQQQ
        if diff_t < -500 and t_c > 0:
            sell_val = min(abs(diff_t), curr_val_t)
            sell_shares = sell_val / t_c
            tqqq_shares -= sell_shares
            cash += sell_val
            sold = True
            trades.append({
                "date": d, "action": "SELL_TQQQ",
                "state_from": prev_state, "state_to": state,
                "amount": sell_val, "shares": sell_shares,
            })

        if sold:
            # T+1 推迟买入
            pending_buy = True
            pending_tg_q = tg_q
            pending_tg_t = tg_t
            trades.append({
                "date": d, "action": "T+1_PENDING",
                "state_from": prev_state, "state_to": state,
                "target_qqq": tg_q, "target_tqqq": tg_t,
            })
        else:
            # 纯买入
            need_q = max(0, target_val_q - curr_val_q)
            need_t = max(0, target_val_t - curr_val_t)

            if need_q > 500 and c > 0:
                buy_shares = min(need_q, cash) / c
                qqq_shares += buy_shares
                cash -= buy_shares * c
                trades.append({
                    "date": d, "action": "BUY_QQQ",
                    "state_from": prev_state, "state_to": state,
                    "amount": buy_shares * c,
                })

            if need_t > 500 and t_c > 0:
                buy_shares = min(need_t, cash) / t_c
                tqqq_shares += buy_shares
                cash -= buy_shares * t_c
                trades.append({
                    "date": d, "action": "BUY_TQQQ",
                    "state_from": prev_state, "state_to": state,
                    "amount": buy_shares * t_c,
                })

        portfolio_log[i] = qqq_shares * c + tqqq_shares * t_c + cash

    # 填充未计算的日志
    for i in range(n):
        if portfolio_log[i] == 0 and i > 0:
            portfolio_log[i] = portfolio_log[i - 1]

    equity_curve = portfolio_log[~np.isnan(portfolio_log) & (portfolio_log > 0)]
    if len(equity_curve) == 0:
        raise ValueError("无法生成净值曲线")

    return equity_curve, dates, trades, merged


def calc_metrics(equity, init_capital=100000.0):
    """计算核心指标"""
    total_return = (equity[-1] / init_capital - 1) * 100
    peak = np.maximum.accumulate(equity)
    dd = (peak - equity) / peak * 100
    max_dd = dd.max()
    cagr = (equity[-1] / init_capital) ** (252 / len(equity)) - 1
    cagr_pct = cagr * 100

    # Sharpe（年化）
    daily_ret = equity[1:] / equity[:-1] - 1
    sharpe = daily_ret.mean() / daily_ret.std() * np.sqrt(252) if daily_ret.std() > 0 else 0

    return {
        "total_return_pct": total_return,
        "max_drawdown_pct": max_dd,
        "cagr_pct": cagr_pct,
        "sharpe": sharpe,
    }


def main():
    print("=" * 90)
    print("  QQQ/TQQQ 双核量化策略 V22.1 — 回测")
    print("=" * 90)

    # 下载数据
    print("\n[1/3] 下载 QQQ / TQQQ 日线数据...")
    df_q = fetch_data("QQQ", start="2015-01-01")
    df_t = fetch_data("TQQQ", start="2015-01-01")
    print(f"  QQQ:  {len(df_q)} 行, {df_q['date'].iloc[0]} ~ {df_q['date'].iloc[-1]}")
    print(f"  TQQQ: {len(df_t)} 行, {df_t['date'].iloc[0]} ~ {df_t['date'].iloc[-1]}")

    # 分阶段回测
    print("\n[2/3] 运行回测...")
    scenarios = [
        ("完整周期 ", "2015-01-01", df_q["date"].iloc[-1]),
    ]

    # 找关键时间段
    for label, start, end in [
        ("2015-2026 完整", date(2015, 1, 1), df_q["date"].iloc[-1]),
        ("2020-2026 新冠后", date(2020, 1, 1), df_q["date"].iloc[-1]),
        ("2022-2026 熊市后", date(2022, 1, 1), df_q["date"].iloc[-1]),
    ]:
        print(f"\n  ── {label} ──")
        eq, dates_arr, trades, merged = run_backtest_v22(
            df_q, df_t, init_capital=100000.0,
            start_date=start, end_date=end,
        )

        metrics = calc_metrics(eq)

        # 买入持有对比
        mask = [(d >= start) & (d <= end) for d in df_q["date"]]
        qq_period = df_q[mask]
        bh_start = float(qq_period.iloc[0]["close"])
        bh_end = float(qq_period.iloc[-1]["close"])
        bh_return = (bh_end / bh_start - 1) * 100

        # TQQQ 买入持有
        mask_t = [(d >= start) & (d <= end) for d in df_t["date"]]
        tq_period = df_t[mask_t]
        t_bh_start = float(tq_period.iloc[0]["close"])
        t_bh_end = float(tq_period.iloc[-1]["close"])
        t_bh_return = (t_bh_end / t_bh_start - 1) * 100

        print(f"    策略收益:   {metrics['total_return_pct']:>+10.1f}%")
        print(f"    QQQ买入持有: {bh_return:>+10.1f}%")
        print(f"    TQQQ买入持有:{t_bh_return:>+10.1f}%")
        print(f"    最大回撤:    {metrics['max_drawdown_pct']:>10.1f}%")
        print(f"    年化收益:    {metrics['cagr_pct']:>10.1f}%")
        print(f"    Sharpe:      {metrics['sharpe']:>10.2f}")
        print(f"    交易次数:    {len(trades):>10}")

    # ── 详细交易记录（完整周期） ──
    print(f"\n[3/3] 完整周期交易明细")
    print("-" * 90)
    eq, dates_arr, trades, merged = run_backtest_v22(
        df_q, df_t, init_capital=100000.0,
        start_date=date(2015, 1, 1), end_date=df_q["date"].iloc[-1],
    )

    # 统计状态分布
    state_changes = [t for t in trades if t.get("state_from")]
    seen_states = set()
    print(f"\n  状态切换记录 ({len(state_changes)} 次):")
    for t in state_changes:
        print(f"    {t['date']}  {t['state_from']:>22s} → {t['state_to']:<22s}  ({t['action']})")

    # 收益对比
    metrics = calc_metrics(eq)
    bh_q = (float(df_q[df_q["date"] >= date(2015, 1, 1)]["close"].iloc[-1]) /
            float(df_q[df_q["date"] >= date(2015, 1, 1)]["close"].iloc[0]) - 1) * 100
    bh_t = (float(df_t[df_t["date"] >= date(2015, 1, 1)]["close"].iloc[-1]) /
            float(df_t[df_t["date"] >= date(2015, 1, 1)]["close"].iloc[0]) - 1) * 100

    print(f"\n  {'=' * 70}")
    print(f"  最终对比 (2015.01 ~ {df_q['date'].iloc[-1]})")
    print(f"  {'─' * 70}")
    print(f"  策略 V22.1:   {metrics['total_return_pct']:>+12.1f}%  回撤 {metrics['max_drawdown_pct']:.1f}%  Sharpe {metrics['sharpe']:.2f}")
    print(f"  QQQ买入持有:  {bh_q:>+12.1f}%")
    print(f"  TQQQ买入持有: {bh_t:>+12.1f}%")
    print()


if __name__ == "__main__":
    main()
