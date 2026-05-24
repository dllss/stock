#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
东方财富市场 ID 工具。
"""


def get_market_id(code: str) -> str:
    """
    根据 A 股代码推导东方财富 secid 的 market id。

    东方财富 K 线接口 secid 格式：
    - 沪市：1.xxxxxx
    - 深市：0.xxxxxx
    - 北交所：2.xxxxxx
    """
    if code.startswith("6"):
        return "1"
    if code.startswith(("4", "8")):
        return "2"
    return "0"
