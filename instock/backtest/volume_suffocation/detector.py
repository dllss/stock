#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""量窒息核心检测逻辑"""

import pandas as pd

from instock.backtest.volume_suffocation.config import PARAMS


def detect_volume_suffocation(df, params=None):
    """
    检测量窒息形态

    量窒息核心逻辑:
        1. 前期放量拉升后，缩量回踩，成交量缩到前期高量的零头（<20%）
        2. 价格基本横盘或微跌，卖方枯竭
        3. 确认信号：收红K / 次日高开 / 支撑位叠加

    Args:
        df: 单只股票的历史K线DataFrame，需含 date/open/close/high/low/volume/p_change 等列
        params: 检测参数字典，默认使用 config.PARAMS

    Returns:
        dict: 检测结果详情，包含 vol_ratio/drawdown/score/confirm_status 等
        None: 不符合量窒息形态
    """
    if params is None:
        params = PARAMS

    if df is None or len(df) < params['min_data_days']:
        return None

    # 确保数据按日期升序
    df = df.sort_values('date').reset_index(drop=True)

    lookback = params['lookback_days']
    if len(df) > lookback:
        df = df.tail(lookback).reset_index(drop=True)

    n = len(df)
    if n < params['min_data_days']:
        return None

    # ==================== 1. 找前期高量 ====================
    high_vol_period = params['high_vol_period']
    high_vol_end = n - params['suffocation_days'] - 2  # 留出窒息区和确认区
    high_vol_start = max(0, high_vol_end - high_vol_period)

    if high_vol_end <= high_vol_start:
        return None

    vol_series = df['volume'].astype(float)
    high_vol_region = vol_series.iloc[high_vol_start:high_vol_end]
    high_vol = high_vol_region.max()

    if high_vol <= 0:
        return None

    # 前期高量对应的日期和价格
    high_vol_idx = high_vol_region.idxmax()
    high_vol_date = df.iloc[high_vol_idx]['date']
    high_vol_close = float(df.iloc[high_vol_idx]['close'])

    # ==================== 2. 量窒息判定 ====================
    suff_days = params['suffocation_days']
    recent_vol = vol_series.iloc[-suff_days:]
    recent_avg_vol = recent_vol.mean()
    recent_max_vol = recent_vol.max()
    recent_min_vol = recent_vol.min()

    vol_ratio = recent_avg_vol / high_vol
    vol_ratio_min = recent_min_vol / high_vol

    is_suffocation = vol_ratio < params['vol_ratio_threshold']
    is_strict_suffocation = vol_ratio < params['vol_ratio_strict']

    if not is_suffocation:
        return None

    # ==================== 3. 价格横盘判定 ====================
    close_series = df['close'].astype(float)
    high_series = df['high'].astype(float)
    low_series = df['low'].astype(float)
    open_series = df['open'].astype(float)

    recent_close = close_series.iloc[-suff_days:]
    recent_high = high_series.iloc[-suff_days:]
    recent_low = low_series.iloc[-suff_days:]

    recent_amplitude = ((recent_high.max() - recent_low.min()) / recent_low.min()) * 100
    price_range_pct = ((recent_high.max() - recent_low.min()) / close_series.iloc[-1]) * 100

    if recent_amplitude > params['max_amplitude_5d']:
        return None
    if price_range_pct > params['max_price_range_5d']:
        return None

    # ==================== 4. 回撤判定 ====================
    period_high = high_series.iloc[high_vol_idx:].max()
    current_close = float(close_series.iloc[-1])
    drawdown_from_high = ((period_high - current_close) / period_high) * 100

    if drawdown_from_high < params['min_drawdown_from_high']:
        return None  # 没有充分调整
    if drawdown_from_high > params['max_drawdown_from_high']:
        return None  # 跌太多

    # ==================== 5. 确认信号判定 ====================
    last_row = df.iloc[-1]
    prev_row = df.iloc[-2] if n >= 2 else None

    # 收红K：收盘价 > 开盘价 或 涨跌幅 > 0
    is_red_k = float(last_row['close']) > float(last_row['open']) or float(last_row['p_change']) > 0

    # 次日高开：最后一天开盘价 > 前一天收盘价
    is_high_open = False
    if prev_row is not None:
        is_high_open = float(last_row['open']) > float(prev_row['close'])

    # 量窒息区间是否出现红K
    suffocation_red = False
    for i in range(-suff_days, 0):
        if float(df.iloc[i]['close']) > float(df.iloc[i]['open']):
            suffocation_red = True
            break

    # ==================== 6. 支撑位判定 ====================
    # 6a. 前低支撑
    period_low = low_series.iloc[:high_vol_end].min()
    near_prev_low = abs(current_close - period_low) / period_low * 100 < 5

    # 6b. 箱体底部支撑
    box_low = recent_low.min()
    near_box_bottom = abs(current_close - box_low) / box_low * 100 < 3

    # 6c. MA支撑
    ma20 = close_series.rolling(20).mean().iloc[-1] if n >= 20 else None
    ma60 = close_series.rolling(60).mean().iloc[-1] if n >= 60 else None
    near_ma20 = ma20 is not None and abs(current_close - ma20) / ma20 * 100 < 3
    above_ma60 = ma60 is not None and current_close > ma60

    # 6d. 大阳线起涨点支撑
    big_up_day = None
    for i in range(high_vol_idx - 1, max(0, high_vol_idx - 20), -1):
        if float(df.iloc[i]['p_change']) > 5:
            big_up_day = float(df.iloc[i]['open'])
            break
    near_big_up = big_up_day is not None and abs(current_close - big_up_day) / big_up_day * 100 < 5

    has_support = near_prev_low or near_box_bottom or near_ma20 or near_big_up

    # ==================== 7. 综合评分 ====================
    score = 0
    reasons = []

    if is_strict_suffocation:
        score += 30
        reasons.append(f"严重量窒息(量比{vol_ratio:.1%})")
    else:
        score += 20
        reasons.append(f"量窒息(量比{vol_ratio:.1%})")

    if is_red_k:
        score += 20
        reasons.append("窒息后收红K")
    if is_high_open:
        score += 15
        reasons.append("次日高开确认")

    if near_prev_low:
        score += 10
        reasons.append("前低支撑")
    if near_box_bottom:
        score += 10
        reasons.append("箱体底部支撑")
    if near_ma20:
        score += 10
        reasons.append("MA20支撑")
    if near_big_up:
        score += 10
        reasons.append("大阳线起涨点支撑")
    if above_ma60:
        score += 5
        reasons.append("站上MA60")

    if recent_amplitude < 5:
        score += 10
        reasons.append(f"极致横盘(振幅{recent_amplitude:.1f}%)")

    # ==================== 8. 确认状态 ====================
    if is_red_k and is_high_open and has_support:
        confirm_status = "\u2605\u2605\u2605 \u4e09\u91cd\u786e\u8ba4"
    elif (is_red_k and has_support) or (is_high_open and has_support):
        confirm_status = "\u2605\u2605 \u53cc\u91cd\u786e\u8ba4"
    elif is_red_k or is_high_open or has_support:
        confirm_status = "\u2605 \u521d\u6b65\u786e\u8ba4"
    else:
        confirm_status = "\u25cb \u5f85\u786e\u8ba4"

    return {
        'vol_ratio': vol_ratio,
        'vol_ratio_min': vol_ratio_min,
        'high_vol': high_vol,
        'high_vol_date': high_vol_date,
        'recent_avg_vol': recent_avg_vol,
        'recent_amplitude': recent_amplitude,
        'price_range_pct': price_range_pct,
        'drawdown_from_high': drawdown_from_high,
        'current_close': current_close,
        'period_high': period_high,
        'is_red_k': is_red_k,
        'is_high_open': is_high_open,
        'has_support': has_support,
        'suffocation_red': suffocation_red,
        'confirm_status': confirm_status,
        'score': score,
        'reasons': ' | '.join(reasons),
        'near_prev_low': near_prev_low,
        'near_box_bottom': near_box_bottom,
        'near_ma20': near_ma20,
        'near_big_up': near_big_up,
        'above_ma60': above_ma60,
        'last_date': last_row['date'],
        'last_change': float(last_row['p_change']),
    }


def analyze_trend_status(df):
    """
    分析单只股票当前趋势状态（用于非量窒息标的的辅助分析）

    Returns:
        dict: 含 vol_ratio/drawdown/status 等
        None: 数据不足
    """
    df = df.sort_values('date').reset_index(drop=True)
    if len(df) < 30:
        return None
    if len(df) > 60:
        df = df.tail(60).reset_index(drop=True)

    close = df['close'].astype(float)
    vol = df['volume'].astype(float)
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    n = len(df)

    # 量比：近5日均量 / 前30天高量
    high_vol_end = n - 7
    high_vol_start = max(0, high_vol_end - 30)
    if high_vol_end <= high_vol_start:
        return None
    high_vol = vol.iloc[high_vol_start:high_vol_end].max()
    recent_avg_vol = vol.iloc[-5:].mean()
    vol_ratio = recent_avg_vol / high_vol if high_vol > 0 else 1.0

    # 趋势状态
    period_high = high.iloc[high_vol_start:].max()
    current_close = close.iloc[-1]
    drawdown = ((period_high - current_close) / period_high) * 100

    change_5d = ((close.iloc[-1] - close.iloc[-6]) / close.iloc[-6]) * 100 if n >= 6 else 0
    change_20d = ((close.iloc[-1] - close.iloc[-21]) / close.iloc[-21]) * 100 if n >= 21 else 0

    ma5 = close.rolling(5).mean().iloc[-1] if n >= 5 else None
    ma20 = close.rolling(20).mean().iloc[-1] if n >= 20 else None
    ma60 = close.rolling(60).mean().iloc[-1] if n >= 60 else None

    recent_amp = ((high.iloc[-5:].max() - low.iloc[-5:].min()) / low.iloc[-5:].min()) * 100

    if drawdown < 5 and change_20d > 10:
        status = '\u5f3a\u52bf\u4e0a\u653b'
    elif drawdown < 15 and vol_ratio < 0.3:
        status = '\u7f29\u91cf\u56de\u8c03\u4e2d'
    elif drawdown > 25:
        status = '\u6df1\u8dcc\u7a81\u7a81\u4f4d'
    elif recent_amp > 12:
        status = '\u5267\u70c8\u6ce2\u52a8'
    elif vol_ratio < 0.35:
        status = '\u63a5\u8fd1\u7a92\u606f'
    else:
        status = '\u653e\u91cf\u9707\u8361'

    return {
        'vol_ratio': vol_ratio,
        'drawdown': drawdown,
        'change_5d': change_5d,
        'change_20d': change_20d,
        'recent_amp': recent_amp,
        'above_ma20': ma20 is not None and current_close > ma20,
        'above_ma60': ma60 is not None and current_close > ma60,
        'ma20': ma20,
        'status': status,
        'current_close': current_close,
    }
