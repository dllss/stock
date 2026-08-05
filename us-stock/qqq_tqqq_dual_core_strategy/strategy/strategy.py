#!/usr/bin/env python3
"""
双核状态机策略（精确移植 base.ts V22.1）。
状态驱动 QQQ / TQQQ 之间的仓位再平衡。
包含：ATH 滚动、逃顶放量过滤、Anti-V 反转过滤（冷静期+MA20斜率）、NORMAL 再平衡。
"""

import pandas as pd

from config.settings import S

# 状态定义（与 base.ts 一致）
STATE_NORMAL = "NORMAL"
STATE_TOP_ESCAPE = "TOP_ESCAPE"
STATE_ZONE_DESPAIR_TQQQ = "ZONE_DESPAIR_TQQQ"
STATE_ZONE_BATTLE_ATTACK = "ZONE_BATTLE_ATTACK"
STATE_ZONE_BATTLE_DEFEND = "ZONE_BATTLE_DEFEND"
STATE_BEAR_CASH = "BEAR_CASH"

RISK_OFF_LIST = [STATE_BEAR_CASH, STATE_ZONE_BATTLE_DEFEND, STATE_TOP_ESCAPE]
RISK_ON_LIST = [STATE_ZONE_BATTLE_ATTACK, STATE_NORMAL]


def target_weights(state: str) -> dict:
    """对齐 base.ts 的 tg_q / tg_t 目标权重。"""
    if state == STATE_ZONE_DESPAIR_TQQQ:
        return {"QQQ": 0.0, "TQQQ": 0.99}
    if state == STATE_ZONE_BATTLE_ATTACK:
        return {"QQQ": 0.0, "TQQQ": 0.99}
    if state == STATE_ZONE_BATTLE_DEFEND:
        return {"QQQ": 0.90, "TQQQ": 0.0}
    if state == STATE_BEAR_CASH:
        return {"QQQ": 0.0, "TQQQ": 0.0}
    if state == STATE_TOP_ESCAPE:
        return {"QQQ": 0.90, "TQQQ": 0.0}
    if state == STATE_NORMAL:
        return {"QQQ": 0.45, "TQQQ": 0.45}
    return {"QQQ": 0.45, "TQQQ": 0.45}


def decide_state(row: pd.Series) -> str:
    """对齐 base.ts handle_data 的 next_state 判定 + Anti-V 过滤。"""
    close_qqq = row["QQQ_Close"]
    ath = row["QQQ_ATH"]
    ma200 = row["QQQ_MA200"]
    ma20 = row["QQQ_MA20"]
    open_qqq = row["QQQ_Open"]
    vol_qqq = row["QQQ_Volume"]
    vol_ma = row["QQQ_VolMA"]

    drawdown = (close_qqq / ath - 1.0) if ath > 0 else 0.0

    # 逃顶信号：close >= ATH*high_zone 且放量(vol>vol_ma*factor) 且收阴(close<open)
    is_top_signal = False
    if ath > 0 and close_qqq >= ath * S.HIGH_ZONE:
        if vol_ma > 0 and vol_qqq > vol_ma * S.VOL_FACTOR:
            if close_qqq < open_qqq:
                is_top_signal = True

    if is_top_signal:
        next_state = STATE_TOP_ESCAPE
    elif ma200 is not None and not pd.isna(ma200) and close_qqq < ma200:
        if drawdown <= -0.30:
            next_state = STATE_ZONE_DESPAIR_TQQQ
        elif drawdown <= -0.10:
            if ma20 is not None and not pd.isna(ma20) and close_qqq > ma20:
                next_state = STATE_ZONE_BATTLE_ATTACK
            else:
                next_state = STATE_ZONE_BATTLE_DEFEND
        else:
            next_state = STATE_BEAR_CASH
    else:
        if drawdown < -0.10:
            next_state = STATE_ZONE_BATTLE_ATTACK
        else:
            next_state = STATE_NORMAL

    return next_state, is_top_signal


def apply_antiv_filter(
    state_label: str, raw_next_state: str, risk_off_days: int, ma20: float, prev_ma20: float
) -> tuple[str, bool, list]:
    """对齐 base.ts 的 Anti-V 反转过滤：risk-off -> risk-on 需冷静期 + MA20 斜率向上。"""
    if state_label not in RISK_OFF_LIST:
        return raw_next_state, False, []
    if raw_next_state not in RISK_ON_LIST:
        return raw_next_state, False, []
    blocked = False
    reasons = []
    if risk_off_days < S.MIN_RISK_OFF_DAYS:
        blocked = True
        reasons.append(f"冷静期未满足：已等待 {risk_off_days} 天，需至少 {S.MIN_RISK_OFF_DAYS} 天")
    if ma20 is not None and not pd.isna(ma20) and prev_ma20 is not None and not pd.isna(prev_ma20):
        if ma20 <= prev_ma20:
            blocked = True
            reasons.append("MA20 斜率仍向下/持平")
    else:
        blocked = True
        reasons.append("MA20 数据不足")
    if blocked:
        return state_label, True, reasons
    return raw_next_state, False, []


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    逐行推进状态机，输出 state / w_QQQ / w_TQQQ。
    用 prev_state 维护风险过滤所需的 risk_off_days。
    """
    df = df.copy()
    states, wq, wt, topflag = [], [], [], []
    prev_state = "INIT"
    risk_off_days = 0

    for _, row in df.iterrows():
        next_state, is_top = decide_state(row)

        # 风险计数（对齐 base.ts）
        if prev_state in (STATE_BEAR_CASH, STATE_ZONE_BATTLE_DEFEND, STATE_TOP_ESCAPE):
            risk_off_days += 1
        else:
            risk_off_days = 0

        next_state, blocked, _ = apply_antiv_filter(
            prev_state, next_state, risk_off_days, row["QQQ_MA20"], row["QQQ_MA20_prev"]
        )

        w = target_weights(next_state)
        states.append(next_state)
        wq.append(w["QQQ"])
        wt.append(w["TQQQ"])
        topflag.append(is_top)
        prev_state = next_state

    df["state"] = states
    df["w_QQQ"] = wq
    df["w_TQQQ"] = wt
    df["is_top_signal"] = topflag
    return df
