#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量窒息选股扫描器 - 全市场入口
================================
扫描全A股中符合量窒息形态的股票。

使用方法:
    cd D:\\WorkProject\\stock
    python -m instock.backtest.volume_suffocation.run_full

输出:
    1. 控制台打印结果
    2. 生成HTML报告（默认输出到 ~/WorkBuddy/）
"""

import os
import sys
import logging
import time
import pandas as pd

# 项目根路径
CPATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
if CPATH not in sys.path:
    sys.path.insert(0, CPATH)

from instock.backtest.volume_suffocation.config import PARAMS
from instock.backtest.volume_suffocation.detector import detect_volume_suffocation
from instock.backtest.volume_suffocation.data_loader import (
    get_latest_trade_date, get_all_stock_codes, batch_load_hist_data,
    get_hist_date_range, filter_stocks,
)
from instock.backtest.volume_suffocation.html_report import generate_full_market_html

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)


def main(output_dir=None):
    """
    全市场量窒息扫描

    Args:
        output_dir: HTML报告输出目录，默认 ~/WorkBuddy/
    """
    logging.info("=" * 60)
    logging.info("\u91cf\u7a92\u606f\u9009\u80a1\u626b\u63cf\u5668 \u542f\u52a8")
    logging.info("=" * 60)

    # 1. 获取最新交易日
    latest_date = get_latest_trade_date()
    logging.info(f"\u6700\u65b0\u4ea4\u6613\u65e5: {latest_date}")

    # 2. 获取所有股票列表
    stocks_df = get_all_stock_codes(latest_date)
    logging.info(f"\u80a1\u7968\u603b\u6570: {len(stocks_df)}")

    # 过滤
    stocks_df = filter_stocks(stocks_df, PARAMS)
    logging.info(f"\u8fc7\u6ee4\u540e\u80a1\u7968\u6570: {len(stocks_df)}")

    # 3. 历史数据范围
    date_start, date_end = get_hist_date_range(latest_date)
    logging.info(f"\u5386\u53f2\u6570\u636e\u8303\u56f4: {date_start} ~ {date_end}")

    # 4. 批量加载历史数据
    logging.info("\u5f00\u59cb\u52a0\u8f7d\u5386\u53f2K\u7ebf\u6570\u636e...")
    t0 = time.time()
    stock_codes = stocks_df['code'].tolist()
    hist_data = batch_load_hist_data(stock_codes, date_start, date_end)
    logging.info(f"\u5386\u53f2\u6570\u636e\u52a0\u8f7d\u5b8c\u6210: {len(hist_data)} \u6761, \u8017\u65f6 {time.time()-t0:.1f}s")

    # 5. 逐股检测量窒息
    logging.info("\u5f00\u59cb\u9010\u80a1\u68c0\u6d4b\u91cf\u7a92\u606f\u5f62\u6001...")
    results = []
    grouped = hist_data.groupby('code')
    total = len(grouped)

    for idx, (code, group_df) in enumerate(grouped, 1):
        if idx % 500 == 0:
            logging.info(f"  \u68c0\u6d4b\u8fdb\u5ea6: {idx}/{total} ({idx*100//total}%)")

        stock_info = stocks_df[stocks_df['code'] == code]
        if stock_info.empty:
            continue
        info = stock_info.iloc[0]

        result = detect_volume_suffocation(group_df)
        if result is None:
            continue

        result['code'] = code
        result['name'] = info['name']
        result['industry'] = info['industry'] if pd.notna(info['industry']) else '\u672a\u77e5'
        result['turnover'] = float(info['turnoverrate']) if pd.notna(info['turnoverrate']) else 0
        result['market_cap'] = float(info['total_market_cap']) if pd.notna(info['total_market_cap']) else 0

        results.append(result)

    logging.info(f"\u68c0\u6d4b\u5b8c\u6210: \u627e\u5230 {len(results)} \u53ea\u91cf\u7a92\u606f\u80a1\u7968")

    if not results:
        logging.warning("\u672a\u627e\u5230\u7b26\u5408\u6761\u4ef6\u7684\u80a1\u7968\uff0c\u8bf7\u68c0\u67e5\u53c2\u6570\u6216\u6570\u636e")
        return

    # 6. 结果排序
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('score', ascending=False).reset_index(drop=True)

    # 7. 打印结果
    print("\n" + "=" * 80)
    print(f"\u91cf\u7a92\u606f\u9009\u80a1\u7ed3\u679c - {latest_date} (\u5171 {len(results_df)} \u53ea)")
    print("=" * 80)

    for i, row in results_df.iterrows():
        print(f"\n[{i+1}] {row['code']} {row['name']} | {row['industry']}")
        print(f"    \u6536\u76d8\u4ef7: {row['current_close']:.2f} | \u8ddd\u9ad8\u70b9\u56de\u64a4: {row['drawdown_from_high']:.1f}%")
        print(f"    \u91cf\u6bd4(\u8fd1\u671f/\u9ad8\u91cf): {row['vol_ratio']:.1%} | \u9ad8\u91cf\u65e5\u671f: {row['high_vol_date']}")
        print(f"    \u8fd15\u65e5\u632f\u5e45: {row['recent_amplitude']:.1f}% | \u786e\u8ba4\u72b6\u6001: {row['confirm_status']}")
        print(f"    \u8bc4\u5206: {row['score']} | \u7406\u7531: {row['reasons']}")

    # 8. 板块统计
    print("\n" + "=" * 80)
    print("\u677f\u5757\u5206\u5e03\u7edf\u8ba1\uff08\u677f\u5757\u7ea7\u91cf\u7a92\u606f = \u540c\u677f\u5757\u591a\u53ea\u4e2a\u80a1\u51fa\u73b0\u91cf\u7a92\u606f\uff09")
    print("=" * 80)
    industry_stats = results_df.groupby('industry').agg(
        count=('code', 'count'),
        avg_score=('score', 'mean'),
        stocks=('name', lambda x: ', '.join(x.tolist()))
    ).sort_values('count', ascending=False)

    for ind, row in industry_stats.iterrows():
        if row['count'] >= 2:
            print(f"  \u3010{ind}\u3011{row['count']}\u53ea  \u5747\u5206{row['avg_score']:.0f}  \u4e2a\u80a1: {row['stocks']}")
        else:
            print(f"  [{ind}] {row['count']}\u53ea  {row['stocks']}")

    # 9. 生成HTML报告
    generate_full_market_html(results_df, industry_stats, latest_date, output_dir)

    return results_df


if __name__ == '__main__':
    main()
