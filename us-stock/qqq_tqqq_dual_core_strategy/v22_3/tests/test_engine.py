#!/usr/bin/env python3
"""V22.3 回测引擎的功能与不变量测试。"""

import os
import sys

import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMON_ROOT = os.path.abspath(os.path.join(ROOT, "..", "common"))
for _path in (ROOT, COMMON_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import backtest.engine as engine  # noqa: E402
import strategy.strategy as ST  # noqa: E402
from config.settings import S  # noqa: E402

@pytest.fixture(scope="session")
def engine_out(trade_df: pd.DataFrame) -> "object":
    """对交易区间跑 engine.run，返回结果命名空间。"""
    sig = ST.generate_signals(trade_df.copy())
    df = sig.copy()
    df["state"] = sig["state"]
    return engine.run(df.copy())


def test_portfolio_positive(engine_out: "object"):
    """总资产应全程为正（无爆仓/负数净值）。"""
    vals = engine_out.portfolio["portfolio_value"]
    assert (vals > 0).all(), "存在非正总资产"


def test_trades_match_positions(engine_out: "object"):
    """每笔交易的金额方向应为正，且交易记录非空。"""
    trades = engine_out.trades  # engine 返回的是 DataFrame
    assert len(trades) > 0, "应有至少一笔交易"
    for t in trades.itertuples():
        assert t.action in ("BUY", "SELL"), f"未知动作: {t.action}"
        assert t.value > 0, "交易金额应为正"


def test_buyhold_benchmark_reasonable(trade_df: pd.DataFrame, engine_out: "object"):
    """双核最终资产应 > 0 且为有限值；与买入持有基准同量级（非数量级偏差）。"""
    final = engine_out.portfolio["portfolio_value"].iloc[-1]
    bh_qqq = engine.buy_and_hold(trade_df.copy(), S.DATA_TICKERS[0])
    bh_tqqq = engine.buy_and_hold(trade_df.copy(), S.DATA_TICKERS[1])
    assert final > 0 and bh_qqq["final_value"] > 0 and bh_tqqq["final_value"] > 0
    # 三者不应出现数量级（10x）异常偏差
    for v in (final, bh_qqq["final_value"], bh_tqqq["final_value"]):
        assert v / final < 50, "与基准数量级异常偏差"

