#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
东方财富 K 线接口通用解析工具。
"""

import pandas as pd


KLINE_COLUMNS = [
    "日期",
    "开盘",
    "收盘",
    "最高",
    "最低",
    "成交量",
    "成交额",
    "振幅",
    "涨跌幅",
    "涨跌额",
    "换手率",
]


def apply_kline_columns(data: pd.DataFrame) -> pd.DataFrame:
    """
    兼容东方财富日 K 接口 11 列和带 f116 时可能出现的 12 列。

    当前接口即使请求 f116 通常也只返回 11 列；
    如果后续接口返回第 12 列，命名为“其他”后丢弃，保持下游字段稳定。
    """
    if len(data.columns) == len(KLINE_COLUMNS):
        data.columns = KLINE_COLUMNS
        return data

    if len(data.columns) == len(KLINE_COLUMNS) + 1:
        data.columns = [*KLINE_COLUMNS, "其他"]
        return data.drop(columns=["其他"])

    raise ValueError(f"Unexpected daily K-line column count: {len(data.columns)}")
