#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量窒息战法选股扫描器
====================
基于量窒息战法方法论，扫描A股中符合量窒息形态的股票。

模块结构:
    config       - 检测参数 + 行业关键词
    detector     - 核心检测逻辑 (detect_volume_suffocation, analyze_trend_status)
    data_loader  - 数据库读取 + 批量加载历史K线
    html_report  - HTML报告生成器 (全市场/科技/AI)
    run_full     - 全市场扫描入口
    run_tech     - 科技板块扫描入口
    run_ai       - AI硬件&软件扫描入口

使用方法:
    cd D:\\WorkProject\\stock
    # 全市场扫描
    python -m instock.backtest.volume_suffocation.run_full
    # 科技板块扫描
    python -m instock.backtest.volume_suffocation.run_tech
    # AI硬件&软件扫描
    python -m instock.backtest.volume_suffocation.run_ai
"""

from instock.backtest.volume_suffocation.config import PARAMS
from instock.backtest.volume_suffocation.detector import (
    detect_volume_suffocation,
    analyze_trend_status,
)
from instock.backtest.volume_suffocation.data_loader import (
    get_latest_trade_date,
    get_all_stock_codes,
    batch_load_hist_data,
)

__all__ = [
    'PARAMS',
    'detect_volume_suffocation',
    'analyze_trend_status',
    'get_latest_trade_date',
    'get_all_stock_codes',
    'batch_load_hist_data',
]
