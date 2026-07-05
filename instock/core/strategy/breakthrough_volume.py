#!/usr/local/bin/python
# -*- coding: utf-8 -*-
"""
策略模块 - 量能突破策略
========================
策略名称：量能突破
策略类型：突破类 + 量价确认

策略原理：
- 股价突破近期高点（N日最高价）
- 可选用放量确认（成交量放大）
- 可选用均线过滤（股价在MA上方）

策略条件（3个可选条件）：
1. 突破条件（核心）：收盘价 >= N日最高价
2. 放量确认（可选，默认开启）：成交量 > MA(vol, M) × 倍数
3. 均线过滤（可选，默认开启）：close > MA(close, K)

参数说明：
- break_days=20  ：突破周期（20日高点突破）
- use_volume=True：是否启用放量确认
- vol_days=5     ：均量周期
- vol_ratio=1.5  ：放量倍数
- use_ma_filter=True：是否启用均线过滤
- ma_days=60     ：均线周期

使用场景：
- 寻找强势突破的股票
- 过滤假突破
- 可灵活组合条件
"""

import numpy as np
import talib as tl

__author__ = 'myh '
__date__ = '2024/06/08 '


def check(
    code_name,
    data,
    date=None,
    threshold=60,
    break_days=20,
    use_volume=True,
    vol_days=5,
    vol_ratio=1.5,
    use_ma_filter=True,
    ma_days=60
):
    """
    量能突破策略检测函数

    参数:
        code_name (tuple): 股票信息 (date, code, name)
        data (DataFrame): 历史K线数据
            - date: 日期
            - open: 开盘价
            - close: 收盘价
            - high: 最高价
            - volume: 成交量
        date (datetime.date, 可选): 计算日期
        threshold (int): 最少需要的数据天数，默认60
        break_days (int): 突破周期，默认20日
        use_volume (bool): 是否启用放量确认，默认True
        vol_days (int): 均量计算周期，默认5日
        vol_ratio (float): 放量倍数阈值，默认1.5倍
        use_ma_filter (bool): 是否启用均线过滤，默认True
        ma_days (int): 均线周期，默认60日

    返回:
        bool: True=符合策略, False=不符合
    """
    # ==================== 步骤1: 数据预处理 ====================
    data = data.copy(deep=True)

    if date is None:
        end_date = code_name[0]
    else:
        end_date = date.strftime("%Y-%m-%d")

    if end_date is not None:
        mask = (data['date'] <= end_date)
        data = data.loc[mask].copy()

    # 需要的数据天数 = max(threshold, break_days, ma_days)
    need_days = max(threshold, break_days, ma_days)
    if len(data.index) < need_days:
        return False

    # ==================== 步骤2: 均线过滤（可选） ====================
    if use_ma_filter:
        close_values = data['close'].values.astype(np.float64)
        data.loc[:, 'ma'] = tl.MA(close_values, timeperiod=ma_days)
        data['ma'].values[np.isnan(data['ma'].values)] = 0.0

        # 取最后一天
        last_close = data.iloc[-1]['close']
        last_ma = data.iloc[-1]['ma']

        # 收盘价必须在均线之上
        if last_ma <= 0 or last_close < last_ma:
            return False

    # ==================== 步骤3: 突破条件（核心） ====================
    # 取最后 break_days+1 天数据
    data_tail = data.tail(n=break_days + 1)

    # 前 break_days 天的最高价
    prev_high = data_tail['high'].values[:-1].max()

    # 最后一天的收盘价
    last_close = data_tail.iloc[-1]['close']

    # 收盘价必须 >= 前N日最高价
    if last_close < prev_high:
        return False

    # ==================== 步骤4: 放量确认（可选） ====================
    if use_volume:
        volume_values = data['volume'].values.astype(np.float64)
        data.loc[:, 'vol_ma'] = tl.MA(volume_values, timeperiod=vol_days)
        data['vol_ma'].values[np.isnan(data['vol_ma'].values)] = 0.0

        last_vol = data.iloc[-1]['volume']
        prev_vol_ma = data.iloc[-2]['vol_ma']  # 用突破前一天的均量

        if prev_vol_ma <= 0:
            return False

        if last_vol / prev_vol_ma < vol_ratio:
            return False

    # ==================== 步骤5: 全部条件满足 ====================
    return True
