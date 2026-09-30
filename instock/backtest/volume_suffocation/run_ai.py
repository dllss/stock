#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量窒息选股扫描器 - AI硬件&软件入口
==================================
扫描AI硬件（半导体/芯片/光电子/元器件/算力设备）和AI软件（软件开发/IT服务/互联网/数字媒体）板块。
若量窒息标的不多，附加分析：接近窒息的票 + AI核心标的当前趋势状态。

使用方法:
    cd D:\\WorkProject\\stock
    python -m instock.backtest.volume_suffocation.run_ai
"""

import os
import sys
import logging
import time
import pandas as pd

CPATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
if CPATH not in sys.path:
    sys.path.insert(0, CPATH)

from instock.backtest.volume_suffocation.config import (
    PARAMS, AI_HARDWARE_INDUSTRIES, AI_SOFTWARE_INDUSTRIES, AI_ALL_KEYWORDS
)
from instock.backtest.volume_suffocation.detector import (
    detect_volume_suffocation, analyze_trend_status
)
from instock.backtest.volume_suffocation.data_loader import (
    get_latest_trade_date, get_all_stock_codes, batch_load_hist_data,
    get_hist_date_range, filter_stocks,
)
from instock.backtest.volume_suffocation.html_report import generate_ai_html

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)


def get_industry_category(industry):
    """返回行业分类：AI硬件 / AI软件 / 其他"""
    if pd.isna(industry):
        return '\u5176\u4ed6'
    ind = str(industry)
    for kw in AI_HARDWARE_INDUSTRIES:
        if kw in ind:
            return 'AI\u786c\u4ef6'
    for kw in AI_SOFTWARE_INDUSTRIES:
        if kw in ind:
            return 'AI\u8f6f\u4ef6'
    return '\u5176\u4ed6'


def is_ai_industry(industry):
    if pd.isna(industry):
        return False
    ind = str(industry)
    for kw in AI_ALL_KEYWORDS:
        if kw in ind:
            return True
    return False


def main(output_dir=None):
    """
    AI硬件&软件量窒息扫描

    Args:
        output_dir: HTML报告输出目录，默认 backtest/volume_suffocation/output/
    """
    logging.info("=" * 60)
    logging.info("\u91cf\u7a92\u606f\u626b\u63cf\u5668 - AI\u786c\u4ef6&\u8f6f\u4ef6\u4e13\u9879 \u542f\u52a8")
    logging.info("=" * 60)

    # 1. 最新交易日
    latest_date = get_latest_trade_date()
    logging.info(f"\u6700\u65b0\u4ea4\u6613\u65e5: {latest_date}")

    # 2. 全市场股票列表
    stocks_df = get_all_stock_codes(latest_date)
    stocks_df = filter_stocks(stocks_df, PARAMS)

    # 3. 筛选AI相关行业
    ai_df = stocks_df[stocks_df['industry'].apply(is_ai_industry)].copy()
    ai_df['category'] = ai_df['industry'].apply(get_industry_category)
    logging.info(f"AI\u786c\u4ef6&\u8f6f\u4ef6\u80a1\u7968\u603b\u6570: {len(ai_df)}")
    logging.info(f"  AI\u786c\u4ef6: {len(ai_df[ai_df['category']=='AI\u786c\u4ef6'])}\u53ea")
    logging.info(f"  AI\u8f6f\u4ef6: {len(ai_df[ai_df['category']=='AI\u8f6f\u4ef6'])}\u53ea")

    # 行业分布
    ai_ind_stats = ai_df.groupby(['category', 'industry']).agg(
        count=('code', 'count')
    ).sort_values('count', ascending=False)
    logging.info("AI\u884c\u4e1a\u5206\u5e03:")
    for (cat, ind), row in ai_ind_stats.iterrows():
        logging.info(f"  [{cat}] {ind}: {row['count']}\u53ea")

    # 4. 加载历史数据
    date_start, date_end = get_hist_date_range(latest_date)
    logging.info("\u5f00\u59cb\u52a0\u8f7dAI\u677f\u5757\u5386\u53f2K\u7ebf\u6570\u636e...")
    t0 = time.time()
    stock_codes = ai_df['code'].tolist()
    hist_data = batch_load_hist_data(stock_codes, date_start, date_end)
    logging.info(f"\u5386\u53f2\u6570\u636e\u52a0\u8f7d\u5b8c\u6210: {len(hist_data)} \u6761, \u8017\u65f6 {time.time()-t0:.1f}s")

    # 5. 逐股检测量窒息 + 趋势分析
    logging.info("\u5f00\u59cb\u9010\u80a1\u68c0\u6d4b\u91cf\u7a92\u606f\u5f62\u6001...")
    results = []
    trend_analysis = []
    grouped = hist_data.groupby('code')
    total = len(grouped)

    for idx, (code, group_df) in enumerate(grouped, 1):
        if idx % 200 == 0:
            logging.info(f"  \u8fdb\u5ea6: {idx}/{total} ({idx*100//total}%)")

        stock_info = ai_df[ai_df['code'] == code]
        if stock_info.empty:
            continue
        info = stock_info.iloc[0]
        category = info['category']

        # 量窒息检测
        result = detect_volume_suffocation(group_df)
        if result is not None:
            result['code'] = code
            result['name'] = info['name']
            result['industry'] = info['industry'] if pd.notna(info['industry']) else '\u672a\u77e5'
            result['category'] = category
            result['turnover'] = float(info['turnoverrate']) if pd.notna(info['turnoverrate']) else 0
            result['market_cap'] = float(info['total_market_cap']) if pd.notna(info['total_market_cap']) else 0
            results.append(result)

        # 趋势状态分析（所有AI股）
        trend = analyze_trend_status(group_df)
        if trend is not None:
            trend['code'] = code
            trend['name'] = info['name']
            trend['industry'] = info['industry'] if pd.notna(info['industry']) else '\u672a\u77e5'
            trend['category'] = category
            trend['market_cap'] = float(info['total_market_cap']) if pd.notna(info['total_market_cap']) else 0
            trend_analysis.append(trend)

    logging.info(f"\u91cf\u7a92\u606f\u8fbe\u6807: {len(results)}\u53ea")
    logging.info(f"\u8d8b\u52bf\u5206\u6790\u5b8c\u6210: {len(trend_analysis)}\u53ea")

    results_df = pd.DataFrame(results)
    if len(results_df) > 0:
        results_df = results_df.sort_values('score', ascending=False).reset_index(drop=True)

    trend_df = pd.DataFrame(trend_analysis)

    # 6. 趋势状态分布统计
    logging.info("\nAI\u677f\u5757\u8d8b\u52bf\u72b6\u6001\u5206\u5e03:")
    status_stats = trend_df.groupby(['category', 'status']).agg(
        count=('code', 'count'),
        avg_drawdown=('drawdown', 'mean'),
        avg_vol_ratio=('vol_ratio', 'mean'),
    ).sort_values('count', ascending=False)
    print("\n" + "=" * 80)
    print("AI\u677f\u5757\u8d8b\u52bf\u72b6\u6001\u5206\u5e03\u7edf\u8ba1")
    print("=" * 80)
    for (cat, status), row in status_stats.iterrows():
        print(f"  [{cat}] {status}: {row['count']}\u53ea  \u5747\u56de\u64a4{row['avg_drawdown']:.1f}%  \u5747\u91cf\u6bd4{row['avg_vol_ratio']:.1%}")

    # 7. 量窒息达标结果
    if len(results_df) > 0:
        print("\n" + "=" * 80)
        print(f"AI\u677f\u5757\u91cf\u7a92\u606f\u8fbe\u6807 - {latest_date} (\u5171 {len(results_df)} \u53ea)")
        print("=" * 80)
        for i, row in results_df.iterrows():
            print(f"\n[{i+1}] {row['code']} {row['name']} | {row['category']} | {row['industry']}")
            print(f"    \u6536\u76d8\u4ef7: {row['current_close']:.2f} | \u56de\u64a4: {row['drawdown_from_high']:.1f}%")
            print(f"    \u91cf\u6bd4: {row['vol_ratio']:.1%} | \u632f\u5e45: {row['recent_amplitude']:.1f}% | \u786e\u8ba4: {row['confirm_status']}")
            print(f"    \u8bc4\u5206: {row['score']} | {row['reasons']}")

    # 8. 接近窒息的（量比20%-40%）
    near_df = trend_df[(trend_df['vol_ratio'] >= 0.20) & (trend_df['vol_ratio'] < 0.40)].copy()
    near_df = near_df.sort_values('vol_ratio')
    print("\n" + "=" * 80)
    print(f"\u63a5\u8fd1\u91cf\u7a92\u606f\u7684AI\u80a1\uff08\u91cf\u6bd420%-40%\uff0c\u6b63\u5728\u7f29\u91cf\u56de\u8c03\u4e2d\uff09- \u5171{len(near_df)}\u53ea")
    print("=" * 80)
    print(f"{'\u4ee3\u7801':8s} {'\u540d\u79f0':8s} {'\u5206\u7c7b':6s} {'\u884c\u4e1a':12s} {'\u4ef7':>8s} {'\u91cf\u6bd4':>7s} {'\u56de\u64a4':>7s} {'5\u65e5\u6da8':>7s} {'20\u65e5\u6da8':>8s} {'\u72b6\u6001'}")
    for _, row in near_df.head(30).iterrows():
        print(f"{row['code']:8s} {row['name']:8s} {row['category']:6s} {row['industry']:12s} {row['current_close']:8.2f} {row['vol_ratio']:6.1%} {row['drawdown']:6.1f}% {row['change_5d']:+6.1f}% {row['change_20d']:+7.1f}% {row['status']}")

    # 9. 还在强势上攻的AI股
    strong_df = trend_df[trend_df['status'] == '\u5f3a\u52bf\u4e0a\u653b'].sort_values('change_20d', ascending=False)
    print("\n" + "=" * 80)
    print(f"\u8fd8\u5728\u5f3a\u52bf\u4e0a\u653b\u7684AI\u80a1\uff08\u6ca1\u56de\u8c03\uff0c\u4e0d\u6ee1\u8db3\u91cf\u7a92\u606f\uff09- \u5171{len(strong_df)}\u53ea TOP 15")
    print("=" * 80)
    print(f"{'\u4ee3\u7801':8s} {'\u540d\u79f0':8s} {'\u5206\u7c7b':6s} {'\u884c\u4e1a':12s} {'\u4ef7':>8s} {'\u91cf\u6bd4':>7s} {'\u56de\u64a4':>7s} {'5\u65e5\u6da8':>7s} {'20\u65e5\u6da8':>8s}")
    for _, row in strong_df.head(15).iterrows():
        print(f"{row['code']:8s} {row['name']:8s} {row['category']:6s} {row['industry']:12s} {row['current_close']:8.2f} {row['vol_ratio']:6.1%} {row['drawdown']:6.1f}% {row['change_5d']:+6.1f}% {row['change_20d']:+7.1f}%")

    # 10. 大市值AI股趋势一览
    big_df = trend_df.sort_values('market_cap', ascending=False).head(30)
    print("\n" + "=" * 80)
    print("\u5927\u5e02\u503cAI\u80a1\u8d8b\u52bf\u72b6\u6001\u4e00\u89c8\uff08\u5e02\u503c\u524d30\uff09")
    print("=" * 80)
    print(f"{'\u4ee3\u7801':8s} {'\u540d\u79f0':8s} {'\u5206\u7c7b':6s} {'\u884c\u4e1a':14s} {'\u4ef7':>8s} {'\u91cf\u6bd4':>7s} {'\u56de\u64a4':>7s} {'5\u65e5\u6da8':>7s} {'20\u65e5\u6da8':>8s} {'MA20':>5s} {'MA60':>5s} {'\u72b6\u6001'}")
    for _, row in big_df.iterrows():
        ma20_flag = '\u2191' if row['above_ma20'] else '\u2193'
        ma60_flag = '\u2191' if row['above_ma60'] else '\u2193'
        print(f"{row['code']:8s} {row['name']:8s} {row['category']:6s} {row['industry']:14s} {row['current_close']:8.2f} {row['vol_ratio']:6.1%} {row['drawdown']:6.1f}% {row['change_5d']:+6.1f}% {row['change_20d']:+7.1f}% {ma20_flag:>5s} {ma60_flag:>5s} {row['status']}")

    # 11. 生成HTML报告
    generate_ai_html(results_df, trend_df, near_df, strong_df, big_df, latest_date, len(ai_df), output_dir)

    # 保存CSV
    from instock.backtest.volume_suffocation.html_report import DEFAULT_OUTPUT_DIR
    csv_dir = output_dir if output_dir else DEFAULT_OUTPUT_DIR
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, 'AI\u677f\u5757\u8d8b\u52bf\u5206\u6790.csv')
    trend_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    logging.info(f"\u8d8b\u52bf\u5206\u6790CSV: {csv_path}")

    return results_df


if __name__ == '__main__':
    main()
