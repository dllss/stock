#!/usr/bin/env python3
"""固化 scripts/verify_alignment.py：当前状态机与 base 复刻版逐日对齐。"""

import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMON_ROOT = os.path.abspath(os.path.join(ROOT, "..", "common"))
for _path in (ROOT, COMMON_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import strategy.strategy as ST  # noqa: E402
from config.settings import S  # noqa: E402


def _base_state_machine(d: pd.DataFrame):
    """base.ts V22 状态机忠实复刻：先判定 next_state -> 累加 risk_off_days -> Anti-V。"""
    states = []
    prev_state = "INIT"
    risk_off_days = 0
    for _, row in d.iterrows():
        close = row["QQQ_Close"]
        ath = row["QQQ_ATH"]
        ma200 = row["QQQ_MA200"]
        ma20 = row["QQQ_MA20"]
        pre_ma20 = row["QQQ_MA20_prev"]
        open_q = row["QQQ_Open"]
        vol = row["QQQ_Volume"]
        vol_ma = row["QQQ_VolMA"]

        drawdown = (close / ath - 1.0) if ath > 0 else 0.0
        is_top = False
        if ath > 0 and close >= ath * S.HIGH_ZONE:
            if vol_ma > 0 and vol > vol_ma * S.VOL_FACTOR:
                if close < open_q:
                    is_top = True
        if is_top:
            nxt = ST.STATE_TOP_ESCAPE
        elif close < ma200:
            if drawdown <= -0.30:
                nxt = ST.STATE_ZONE_DESPAIR_TQQQ
            elif drawdown <= -0.10:
                nxt = ST.STATE_ZONE_BATTLE_ATTACK if close > ma20 else ST.STATE_ZONE_BATTLE_DEFEND
            else:
                nxt = ST.STATE_BEAR_CASH
        else:
            nxt = ST.STATE_ZONE_BATTLE_ATTACK if drawdown < -0.10 else ST.STATE_NORMAL

        if prev_state in ST.RISK_OFF_LIST:
            risk_off_days += 1
        else:
            risk_off_days = 0

        if prev_state in ST.RISK_OFF_LIST and nxt in ST.RISK_ON_LIST:
            blocked = False
            if risk_off_days < S.MIN_RISK_OFF_DAYS:
                blocked = True
            if (
                ma20 is not None
                and not pd.isna(ma20)
                and pre_ma20 is not None
                and not pd.isna(pre_ma20)
            ):
                if ma20 <= pre_ma20:
                    blocked = True
            else:
                blocked = True
            if blocked:
                nxt = prev_state

        states.append(nxt)
        prev_state = nxt
    return states


def test_state_machine_aligns_with_base(trade_df: pd.DataFrame):
    """当前 generate_signals 必须与 base 复刻版逐日完全一致。"""
    base_states = _base_state_machine(trade_df)
    cur = ST.generate_signals(trade_df.copy())
    cur_states = cur["state"].tolist()

    assert len(base_states) == len(cur_states), "交易日数量不一致"
    mismatch = sum(1 for b, c in zip(base_states, cur_states) if b != c)
    assert mismatch == 0, f"状态机与 base 不一致天数: {mismatch}"
