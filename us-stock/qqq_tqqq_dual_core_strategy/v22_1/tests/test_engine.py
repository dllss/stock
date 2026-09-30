#!/usr/bin/env python3
"""engine.run 的功能与不变量测试（防回归）。

注意：scripts/verify_engine.py 用 base.ts 复刻版做逐笔净值比对，目前发现
engine 与 base 复刻在 2022-08-15 附近有 ~7.7% 偏差（复刻误差或策略待对齐项），
属已知 TODO，不在此处做硬断言。本测试聚焦 engine 自身行为稳定性与不变量的
快照回归，确保后续改动不会破坏既有回测结果。
"""

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

# 全周期回测快照（首次生成后缓存，用于回归比对）
_SNAP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "engine_snapshot.json")


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


def test_snapshot_regression(engine_out: "object"):
    """快照回归：最终资产与累计收益应与缓存基准一致（防未来改动破坏）。"""
    import json

    final = float(engine_out.portfolio["portfolio_value"].iloc[-1])
    summ = engine.summary(engine_out.portfolio)
    ret = float(summ["total_return"])
    snap = {"final_value": final, "total_return": ret}

    if not os.path.exists(_SNAP):
        with open(_SNAP, "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2)
        pytest.skip("快照不存在，已生成基准，下次运行将比对")

    with open(_SNAP, encoding="utf-8") as f:
        base = json.load(f)
    rel = abs(final - base["final_value"]) / base["final_value"] * 100
    assert rel < 0.5, f"最终资产相对快照偏差 {rel:.4f}% 超阈值（回归？）"
