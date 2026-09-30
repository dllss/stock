#!/usr/bin/env python3
"""V22.3 状态机的关键规则测试。"""

import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMON_ROOT = os.path.abspath(os.path.join(ROOT, "..", "common"))
for _path in (ROOT, COMMON_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import strategy.strategy as ST  # noqa: E402


def _row(close, ma200, ma20, prev_ma20, ath=100.0, volume=1.0, vol_ma=1.0):
    return pd.Series(
        {
            "QQQ_Close": close,
            "QQQ_Open": close + 1.0,
            "QQQ_ATH": ath,
            "QQQ_MA200": ma200,
            "QQQ_MA20": ma20,
            "QQQ_MA20_prev": prev_ma20,
            "QQQ_Volume": volume,
            "QQQ_VolMA": vol_ma,
        }
    )


def test_v22_3_normal_weights():
    assert ST.target_weights(ST.STATE_NORMAL) == {"QQQ": 0.0, "TQQQ": 0.90}


def test_hi_chain_and_deep_drawdown_gate():
    state, _ = ST.decide_state(_row(125.0, 100.0, 115.0, 114.0))
    assert state == ST.STATE_HI

    state, _ = ST.decide_state(_row(112.0, 100.0, 115.0, 116.0), ST.STATE_HI)
    assert state == ST.STATE_HI_CASH

    state, _ = ST.decide_state(_row(118.0, 100.0, 115.0, 114.0), ST.STATE_HI_CASH)
    assert state == ST.STATE_NORMAL

    state, _ = ST.decide_state(_row(60.0, 100.0, 90.0, 89.0))
    assert state == ST.STATE_ZONE_DESPAIR_TQQQ

    state, _ = ST.decide_state(_row(60.0, 100.0, 90.0, 91.0))
    assert state == ST.STATE_ZONE_BATTLE_DEFEND
