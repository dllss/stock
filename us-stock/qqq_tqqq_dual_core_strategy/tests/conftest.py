#!/usr/bin/env python3
"""pytest 公共 fixture：带缓存的合并行情数据。

首次运行需联网从东方财富拉取（约数秒），结果缓存到 tests/data/merged_cache.csv，
之后离线即可跑，CI 友好。设置环境变量 SKIP_NETWORK_TESTS=1 可跳过需联网的用例。
"""

import os
import sys

import pandas as pd
import pytest

# 让 tests/ 能 import 到项目根包（config / data / strategy / backtest / utils）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from config.settings import S  # noqa: E402
from data import fetcher  # noqa: E402
from utils import helpers  # noqa: E402

_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "merged_cache.csv")


def _fetch_fresh() -> pd.DataFrame:
    raw = fetcher.fetch_and_merge(S.DATA_TICKERS, start="2010-01-01", end="2026-08-03")
    os.makedirs(os.path.dirname(_CACHE), exist_ok=True)
    raw.to_csv(_CACHE, index=False)
    return raw


@pytest.fixture(scope="session")
def merged_df() -> pd.DataFrame:
    """带缓存的合并行情（含技术指标），覆盖长周期影响区。"""
    if os.path.exists(_CACHE):
        df = pd.read_csv(_CACHE, parse_dates=["date"])
    else:
        if os.environ.get("SKIP_NETWORK_TESTS"):
            pytest.skip("SKIP_NETWORK_TESTS 已设置且缓存不存在，跳过需联网的用例")
        df = _fetch_fresh()
    return helpers.add_indicators(df)


@pytest.fixture(scope="session")
def trade_df(merged_df: pd.DataFrame) -> pd.DataFrame:
    """交易区间数据（WARMUP_END 之后，指标已就绪），与 main.py 一致。"""
    return merged_df[merged_df["date"] >= pd.to_datetime(S.WARMUP_END)].reset_index(drop=True)
