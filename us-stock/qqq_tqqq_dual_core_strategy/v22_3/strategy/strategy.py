#!/usr/bin/env python3
"""纳指四季 V22.3 的本地回测状态机。

状态判定与 ``us-stock/python.py`` 的 V22.3 保持一致，回测引擎只负责
按状态目标仓位模拟交易。
"""

import pandas as pd

from config.settings import S


STATE_NORMAL = "NORMAL"
STATE_TOP_ESCAPE = "TOP_ESCAPE"
STATE_HI = "HI"
STATE_HI_CASH = "HI_CASH"
STATE_ZONE_DESPAIR_TQQQ = "ZONE_DESPAIR_TQQQ"
STATE_ZONE_BATTLE_ATTACK = "ZONE_BATTLE_ATTACK"
STATE_ZONE_BATTLE_DEFEND = "ZONE_BATTLE_DEFEND"
STATE_BEAR_CASH = "BEAR_CASH"

RISK_OFF_LIST = [STATE_BEAR_CASH, STATE_ZONE_BATTLE_DEFEND, STATE_TOP_ESCAPE]
RISK_ON_LIST = [STATE_ZONE_BATTLE_ATTACK, STATE_NORMAL]


def _valid(value):
    return value is not None and not pd.isna(value)


def target_weights(state: str) -> dict:
    """返回 V22.3 各状态的目标仓位。"""
    if state in (STATE_ZONE_DESPAIR_TQQQ, STATE_ZONE_BATTLE_ATTACK):
        return {"QQQ": 0.0, "TQQQ": 0.99}
    if state == STATE_ZONE_BATTLE_DEFEND:
        return {"QQQ": 0.90, "TQQQ": 0.0}
    if state == STATE_BEAR_CASH or state == STATE_HI_CASH:
        return {"QQQ": 0.0, "TQQQ": 0.0}
    if state == STATE_TOP_ESCAPE:
        return {"QQQ": 0.90, "TQQQ": 0.0}
    if state == STATE_HI:
        return {"QQQ": 1.0, "TQQQ": 0.0}
    if state == STATE_NORMAL:
        # V22.3: NORMAL 改为 90% TQQQ，保留 10% 现金。
        return {"QQQ": 0.0, "TQQQ": 0.90}
    return {"QQQ": 0.0, "TQQQ": 0.90}


def _top_signal(row: pd.Series) -> bool:
    close = row["QQQ_Close"]
    ath = row["QQQ_ATH"]
    vol = row["QQQ_Volume"]
    vol_ma = row["QQQ_VolMA"]
    open_price = row["QQQ_Open"]
    return (
        _valid(ath)
        and ath > 0
        and close >= ath * S.HIGH_ZONE
        and _valid(vol_ma)
        and vol_ma > 0
        and vol > vol_ma * S.VOL_FACTOR
        and close < open_price
    )


def decide_state(row: pd.Series, state_label: str = "INIT") -> tuple[str, bool]:
    """执行 V22.3 的状态判定，并返回 ``(状态, 是否触发放量逃顶)``。"""
    close = row["QQQ_Close"]
    ath = row["QQQ_ATH"]
    ma200 = row["QQQ_MA200"]
    ma20 = row["QQQ_MA20"]
    prev_ma20 = row["QQQ_MA20_prev"]
    drawdown = (close / ath - 1.0) if _valid(ath) and ath > 0 else 0.0

    above_ma200 = _valid(ma200) and close > ma200
    above_ma20 = _valid(ma20) and close > ma20
    below_ma20 = _valid(ma20) and close < ma20
    ma20_rising = _valid(ma20) and _valid(prev_ma20) and ma20 > prev_ma20
    ma20_falling = _valid(ma20) and _valid(prev_ma20) and ma20 < prev_ma20
    is_hi_deviation = (
        _valid(ma200)
        and ma200 > 0
        and close > ma200 * (1.0 + S.HI_DEVIATION_PCT)
    )
    is_top = _top_signal(row)

    # V22.2 的 HI/HI_CASH 解锁链，V22.3 保持不变。
    if state_label == STATE_HI and above_ma200:
        if below_ma20 and ma20_falling:
            return STATE_HI_CASH, is_top
        return STATE_HI, is_top
    if state_label == STATE_HI_CASH and above_ma200:
        if above_ma20:
            return STATE_NORMAL, is_top
        return STATE_HI_CASH, is_top

    # 先处理 V22.2 的乖离逃顶，再进入 V22.1 的风险区状态机。
    if is_hi_deviation:
        return STATE_HI, is_top
    if is_top:
        return STATE_TOP_ESCAPE, is_top
    if _valid(ma200) and close < ma200:
        if drawdown <= S.DESPAIR_LINE:
            # 深坑只有 MA20 向上才允许进入 TQQQ 进攻状态。
            return (
                STATE_ZONE_DESPAIR_TQQQ if ma20_rising else STATE_ZONE_BATTLE_DEFEND,
                is_top,
            )
        if drawdown <= S.BATTLE_LINE:
            return (
                STATE_ZONE_BATTLE_ATTACK if above_ma20 else STATE_ZONE_BATTLE_DEFEND,
                is_top,
            )
        return STATE_BEAR_CASH, is_top

    if drawdown < S.BATTLE_LINE:
        return STATE_ZONE_BATTLE_ATTACK, is_top
    return STATE_NORMAL, is_top


def apply_antiv_filter(
    state_label: str, raw_next_state: str, risk_off_days: int, ma20, prev_ma20
) -> tuple[str, bool, list]:
    """风险区恢复进攻/常态时，应用冷静期和 MA20 斜率过滤。"""
    if state_label not in RISK_OFF_LIST or raw_next_state not in RISK_ON_LIST:
        return raw_next_state, False, []

    blocked = []
    if risk_off_days < S.MIN_RISK_OFF_DAYS:
        blocked.append(
            f"冷静期未满足：已等待 {risk_off_days} 天，需至少 {S.MIN_RISK_OFF_DAYS} 天"
        )
    if not (_valid(ma20) and _valid(prev_ma20)):
        blocked.append("MA20 数据不足")
    elif ma20 <= prev_ma20:
        blocked.append("MA20 斜率仍向下/持平")

    if blocked:
        return state_label, True, blocked
    return raw_next_state, False, []


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """逐行推进 V22.3 状态机并附加目标仓位。"""
    df = df.copy()
    states, wq, wt, topflag = [], [], [], []
    prev_state = "INIT"
    risk_off_days = 0

    for _, row in df.iterrows():
        next_state, is_top = decide_state(row, prev_state)

        if prev_state in RISK_OFF_LIST:
            risk_off_days += 1
        else:
            risk_off_days = 0

        next_state, _, _ = apply_antiv_filter(
            prev_state,
            next_state,
            risk_off_days,
            row["QQQ_MA20"],
            row["QQQ_MA20_prev"],
        )

        weights = target_weights(next_state)
        states.append(next_state)
        wq.append(weights["QQQ"])
        wt.append(weights["TQQQ"])
        topflag.append(is_top)
        prev_state = next_state

    df["state"] = states
    df["w_QQQ"] = wq
    df["w_TQQQ"] = wt
    df["is_top_signal"] = topflag
    return df
