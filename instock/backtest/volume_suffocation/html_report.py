#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HTML报告生成器 - 全市场 / 科技板块 / AI硬件软件"""

import os
import logging
from datetime import datetime

# 默认输出目录：包目录下的 output/
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')


def _confirm_color(status):
    """根据确认状态返回颜色"""
    if '\u2605\u2605\u2605' in status:
        return 'red'
    elif '\u2605\u2605' in status:
        return 'orange'
    elif '\u2605' in status:
        return 'blue'
    return 'gray'


def _status_color(status):
    """趋势状态颜色映射"""
    mapping = {
        '\u7f29\u91cf\u56de\u8c03\u4e2d': '#4caf50',
        '\u63a5\u8fd1\u7a92\u606f': '#ff9800',
        '\u5f3a\u52bf\u4e0a\u653b': '#f44336',
        '\u6df1\u8dcc\u7a81\u4f4d': '#9e9e9e',
        '\u5267\u70c8\u6ce2\u52a8': '#ff5722',
        '\u653e\u91cf\u9707\u8361': '#607d8b',
    }
    return mapping.get(status, '#333')


def _cat_color(category):
    """AI分类颜色"""
    return '#e91e63' if category == 'AI\u786c\u4ef6' else '#9c27b0'


# ==================== 全市场报告 ====================

def generate_full_market_html(results_df, industry_stats, latest_date, output_dir=None):
    """生成全市场量窒息扫描HTML报告"""
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    output_path = os.path.join(output_dir, '\u91cf\u7a92\u606f\u9009\u80a1\u7ed3\u679c.html')

    table_rows = []
    for i, row in results_df.iterrows():
        table_rows.append(f"""
        <tr>
            <td>{i+1}</td>
            <td class="code">{row['code']}</td>
            <td class="name">{row['name']}</td>
            <td>{row['industry']}</td>
            <td>{row['current_close']:.2f}</td>
            <td>{row['last_change']:+.2f}%</td>
            <td>{row['vol_ratio']:.1%}</td>
            <td>{row['drawdown_from_high']:.1f}%</td>
            <td>{row['recent_amplitude']:.1f}%</td>
            <td class="confirm" style="color:{_confirm_color(row['confirm_status'])}">{row['confirm_status']}</td>
            <td class="score">{row['score']}</td>
            <td class="reasons">{row['reasons']}</td>
        </tr>""")

    sector_rows = []
    for ind, row in industry_stats.iterrows():
        highlight = 'sector-hot' if row['count'] >= 3 else ('sector-warm' if row['count'] >= 2 else '')
        sector_rows.append(f"""
        <tr class="{highlight}">
            <td>{ind}</td>
            <td>{row['count']}</td>
            <td>{row['avg_score']:.0f}</td>
            <td>{row['stocks']}</td>
        </tr>""")

    triple_count = len(results_df[results_df['confirm_status'].str.contains('\u2605\u2605\u2605')])
    double_count = len(results_df[results_df['confirm_status'].str.contains('\u2605\u2605') & ~results_df['confirm_status'].str.contains('\u2605\u2605\u2605')])
    sector_count = len(industry_stats[industry_stats['count'] >= 3])

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>\u91cf\u7a92\u606f\u9009\u80a1\u7ed3\u679c - {latest_date}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Microsoft YaHei', 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; padding: 20px; }}
h1 {{ color: #c41e3a; margin-bottom: 5px; }}
.subtitle {{ color: #666; margin-bottom: 20px; font-size: 14px; }}
.summary {{ display: flex; gap: 15px; margin-bottom: 20px; }}
.summary-card {{ background: white; padding: 15px 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; }}
.summary-card .label {{ font-size: 12px; color: #999; }}
.summary-card .value {{ font-size: 24px; font-weight: bold; color: #c41e3a; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; font-size: 13px; }}
th {{ background: #c41e3a; color: white; padding: 10px 8px; text-align: left; font-weight: 500; white-space: nowrap; }}
td {{ padding: 8px; border-bottom: 1px solid #eee; }}
tr:hover {{ background: #fff5f5; }}
.code {{ font-family: monospace; font-weight: bold; }}
.name {{ font-weight: bold; }}
.confirm {{ font-weight: bold; white-space: nowrap; }}
.score {{ font-weight: bold; color: #c41e3a; text-align: center; }}
.reasons {{ font-size: 12px; color: #666; max-width: 300px; }}
.sector-hot {{ background: #fff0f0 !important; font-weight: bold; }}
.sector-warm {{ background: #fff8f8 !important; }}
h2 {{ color: #333; margin: 20px 0 10px; font-size: 18px; }}
.legend {{ background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; font-size: 13px; line-height: 1.8; }}
.legend strong {{ color: #c41e3a; }}
</style>
</head>
<body>
<h1>\u91cf\u7a92\u606f\u9009\u80a1\u7ed3\u679c</h1>
<p class="subtitle">\u626b\u63cf\u65e5\u671f: {latest_date} | \u751f\u6210\u65f6\u95f4: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

<div class="summary">
    <div class="summary-card"><div class="label">\u7b26\u5408\u91cf\u7a92\u606f\u80a1\u7968\u6570</div><div class="value">{len(results_df)}</div></div>
    <div class="summary-card"><div class="label">\u4e09\u91cd\u786e\u8ba4\uff08\u91cd\u70b9\u5173\u6ce8\uff09</div><div class="value">{triple_count}</div></div>
    <div class="summary-card"><div class="label">\u53cc\u91cd\u786e\u8ba4</div><div class="value">{double_count}</div></div>
    <div class="summary-card"><div class="label">\u677f\u5757\u7ea7\u91cf\u7a92\u606f\uff08\u22653\u53ea\uff09</div><div class="value">{sector_count}</div></div>
</div>

<div class="legend">
    <strong>\u91cf\u6bd4</strong> = \u8fd1\u671f5\u65e5\u5747\u91cf / \u524d\u671f\u9ad8\u91cf\uff0c\u8d8a\u4f4e\u8d8a\u7a92\u606f\uff08&lt;20%\u4e3a\u91cf\u7a92\u606f\uff0c&lt;15%\u4e3a\u4e25\u91cd\u7a92\u606f\uff09<br>
    <strong>\u786e\u8ba4\u72b6\u6001</strong>\uff1a\u2605\u2605\u2605\u4e09\u91cd\u786e\u8ba4\uff08\u6536\u7ea2+\u9ad8\u5f00+\u652f\u6491\uff09| \u2605\u2605\u53cc\u91cd\u786e\u8ba4 | \u2605\u521d\u6b65\u786e\u8ba4 | \u25cb\u5f85\u786e\u8ba4<br>
    <strong>\u8bc4\u5206</strong>\uff1a\u7efc\u5408\u91cf\u7a92\u606f\u7a0b\u5ea6\u3001\u786e\u8ba4\u4fe1\u53f7\u3001\u652f\u6491\u4f4d\u3001\u6a2a\u76d8\u8d28\u91cf\u7684\u52a0\u6743\u8bc4\u5206\uff0c\u8d8a\u9ad8\u8d8a\u503c\u5f97\u5173\u6ce8<br>
    <strong>\u6ce8\u610f</strong>\uff1a\u91cf\u7a92\u606f\u662f"\u63a5\u8fd1\u6b62\u8dcc"\u4fe1\u53f7\uff0c\u975e\u4e70\u5165\u4fe1\u53f7\u3002\u9700\u7b49\u786e\u8ba4\u6761\u4ef6\u6ee1\u8db3\u540e\u518d\u8003\u8651\u4ecb\u5165\u3002
</div>

<h2>\u4e2a\u80a1\u660e\u7ec6\uff08\u6309\u8bc4\u5206\u6392\u5e8f\uff09</h2>
<table>
<thead><tr><th>#</th><th>\u4ee3\u7801</th><th>\u540d\u79f0</th><th>\u884c\u4e1a</th><th>\u6536\u76d8\u4ef7</th><th>\u6da8\u8dcc\u5e45</th><th>\u91cf\u6bd4</th><th>\u56de\u64a4</th><th>5\u65e5\u632f\u5e45</th><th>\u786e\u8ba4\u72b6\u6001</th><th>\u8bc4\u5206</th><th>\u7406\u7531</th></tr></thead>
<tbody>
{''.join(table_rows)}
</tbody>
</table>

<h2>\u677f\u5757\u5206\u5e03\u7edf\u8ba1</h2>
<table>
<thead><tr><th>\u884c\u4e1a\u677f\u5757</th><th>\u6570\u91cf</th><th>\u5e73\u5747\u8bc4\u5206</th><th>\u4e2a\u80a1</th></tr></thead>
<tbody>
{''.join(sector_rows)}
</tbody>
</table>

</body>
</html>"""

    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logging.info(f"HTML\u62a5\u544a\u5df2\u751f\u6210: {output_path}")
    return output_path


# ==================== 科技板块报告 ====================

def generate_tech_html(results_df, industry_stats, latest_date, tech_total, output_dir=None):
    """生成科技板块量窒息扫描HTML报告"""
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    output_path = os.path.join(output_dir, '\u79d1\u6280\u677f\u5757\u91cf\u7a92\u606f\u7ed3\u679c.html')

    table_rows = []
    for i, row in results_df.iterrows():
        table_rows.append(f"""
        <tr>
            <td>{i+1}</td>
            <td class="code">{row['code']}</td>
            <td class="name">{row['name']}</td>
            <td>{row['industry']}</td>
            <td>{row['current_close']:.2f}</td>
            <td>{row['last_change']:+.2f}%</td>
            <td>{row['vol_ratio']:.1%}</td>
            <td>{row['drawdown_from_high']:.1f}%</td>
            <td>{row['recent_amplitude']:.1f}%</td>
            <td class="confirm" style="color:{_confirm_color(row['confirm_status'])}">{row['confirm_status']}</td>
            <td class="score">{row['score']}</td>
            <td class="reasons">{row['reasons']}</td>
        </tr>""")

    sector_rows = []
    for ind, row in industry_stats.iterrows():
        highlight = 'sector-hot' if row['count'] >= 3 else ('sector-warm' if row['count'] >= 2 else '')
        sector_rows.append(f"""
        <tr class="{highlight}">
            <td>{ind}</td>
            <td>{row['count']}</td>
            <td>{row['avg_score']:.0f}</td>
            <td>{row['stocks']}</td>
        </tr>""")

    triple_count = len(results_df[results_df['confirm_status'].str.contains('\u2605\u2605\u2605')])
    double_count = len(results_df[results_df['confirm_status'].str.contains('\u2605\u2605') & ~results_df['confirm_status'].str.contains('\u2605\u2605\u2605')])
    sector_count = len(industry_stats[industry_stats['count'] >= 3])

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>\u79d1\u6280\u677f\u5757\u91cf\u7a92\u606f\u9009\u80a1\u7ed3\u679c - {latest_date}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Microsoft YaHei', 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; padding: 20px; }}
h1 {{ color: #1a73e8; margin-bottom: 5px; }}
.subtitle {{ color: #666; margin-bottom: 20px; font-size: 14px; }}
.summary {{ display: flex; gap: 15px; margin-bottom: 20px; }}
.summary-card {{ background: white; padding: 15px 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; }}
.summary-card .label {{ font-size: 12px; color: #999; }}
.summary-card .value {{ font-size: 24px; font-weight: bold; color: #1a73e8; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; font-size: 13px; }}
th {{ background: #1a73e8; color: white; padding: 10px 8px; text-align: left; font-weight: 500; white-space: nowrap; }}
td {{ padding: 8px; border-bottom: 1px solid #eee; }}
tr:hover {{ background: #f0f7ff; }}
.code {{ font-family: monospace; font-weight: bold; }}
.name {{ font-weight: bold; }}
.confirm {{ font-weight: bold; white-space: nowrap; }}
.score {{ font-weight: bold; color: #1a73e8; text-align: center; }}
.reasons {{ font-size: 12px; color: #666; max-width: 300px; }}
.sector-hot {{ background: #e8f2ff !important; font-weight: bold; }}
.sector-warm {{ background: #f0f7ff !important; }}
h2 {{ color: #333; margin: 20px 0 10px; font-size: 18px; }}
.legend {{ background: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; font-size: 13px; line-height: 1.8; }}
.legend strong {{ color: #1a73e8; }}
.note {{ background: #fff3cd; border: 1px solid #ffc107; padding: 12px 15px; border-radius: 8px; margin-bottom: 20px; font-size: 13px; }}
</style>
</head>
<body>
<h1>\u79d1\u6280\u677f\u5757\u91cf\u7a92\u606f\u9009\u80a1\u7ed3\u679c</h1>
<p class="subtitle">\u626b\u63cf\u65e5\u671f: {latest_date} | \u79d1\u6280\u80a1\u603b\u6570: {tech_total} | \u751f\u6210\u65f6\u95f4: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

<div class="summary">
    <div class="summary-card"><div class="label">\u7b26\u5408\u91cf\u7a92\u606f\u79d1\u6280\u80a1</div><div class="value">{len(results_df)}</div></div>
    <div class="summary-card"><div class="label">\u4e09\u91cd\u786e\u8ba4</div><div class="value">{triple_count}</div></div>
    <div class="summary-card"><div class="label">\u53cc\u91cd\u786e\u8ba4</div><div class="value">{double_count}</div></div>
    <div class="summary-card"><div class="label">\u677f\u5757\u7ea7\u91cf\u7a92\u606f\uff08\u22653\u53ea\uff09</div><div class="value">{sector_count}</div></div>
</div>

<div class="legend">
    <strong>\u91cf\u6bd4</strong> = \u8fd1\u671f5\u65e5\u5747\u91cf / \u524d\u671f\u9ad8\u91cf\uff0c\u8d8a\u4f4e\u8d8a\u7a92\u606f\uff08&lt;20%\u4e3a\u91cf\u7a92\u606f\uff0c&lt;15%\u4e3a\u4e25\u91cd\u7a92\u606f\uff09<br>
    <strong>\u786e\u8ba4\u72b6\u6001</strong>\uff1a\u2605\u2605\u2605\u4e09\u91cd\u786e\u8ba4\uff08\u6536\u7ea2+\u9ad8\u5f00+\u652f\u6491\uff09| \u2605\u2605\u53cc\u91cd\u786e\u8ba4 | \u2605\u521d\u6b65\u786e\u8ba4 | \u25cb\u5f85\u786e\u8ba4<br>
    <strong>\u8bc4\u5206</strong>\uff1a\u7efc\u5408\u91cf\u7a92\u606f\u7a0b\u5ea6\u3001\u786e\u8ba4\u4fe1\u53f7\u3001\u652f\u6491\u4f4d\u3001\u6a2a\u76d8\u8d28\u91cf\u7684\u52a0\u6743\u8bc4\u5206<br>
    <strong>\u79d1\u6280\u884c\u4e1a\u8303\u56f4</strong>\uff1a\u534a\u5bfc\u4f53\u3001\u7535\u5b50\u3001\u901a\u4fe1\u3001\u8ba1\u7b97\u673a\u3001\u8f6f\u4ef6\u3001\u4e92\u8054\u7f51\u3001\u6d88\u8d39\u7535\u5b50\u3001\u5149\u5b66\u3001\u5143\u5668\u4ef6\u3001\u65b0\u80fd\u6e90\u3001\u519b\u5de5\u3001\u533b\u7597\u7b49
</div>

<h2>\u4e2a\u80a1\u660e\u7ec6\uff08\u6309\u8bc4\u5206\u6392\u5e8f\uff09</h2>
<table>
<thead><tr><th>#</th><th>\u4ee3\u7801</th><th>\u540d\u79f0</th><th>\u884c\u4e1a</th><th>\u6536\u76d8\u4ef7</th><th>\u6da8\u8dcc\u5e45</th><th>\u91cf\u6bd4</th><th>\u56de\u64a4</th><th>5\u65e5\u632f\u5e45</th><th>\u786e\u8ba4\u72b6\u6001</th><th>\u8bc4\u5206</th><th>\u7406\u7531</th></tr></thead>
<tbody>
{''.join(table_rows)}
</tbody>
</table>

<h2>\u79d1\u6280\u5b50\u884c\u4e1a\u5206\u5e03</h2>
<table>
<thead><tr><th>\u884c\u4e1a\u677f\u5757</th><th>\u6570\u91cf</th><th>\u5e73\u5747\u8bc4\u5206</th><th>\u4e2a\u80a1</th></tr></thead>
<tbody>
{''.join(sector_rows)}
</tbody>
</table>

</body>
</html>"""

    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logging.info(f"HTML\u62a5\u544a\u5df2\u751f\u6210: {output_path}")
    return output_path


# ==================== AI硬件&软件报告 ====================

def generate_ai_html(results_df, trend_df, near_df, strong_df, big_df, latest_date, ai_total, output_dir=None):
    """生成AI硬件&软件量窒息分析HTML报告"""
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR

    output_path = os.path.join(output_dir, 'AI\u786c\u4ef6\u8f6f\u4ef6\u91cf\u7a92\u606f\u7ed3\u679c.html')

    # 量窒息达标表格
    suff_rows = []
    if len(results_df) > 0:
        for i, row in results_df.iterrows():
            suff_rows.append(f"""
            <tr>
                <td>{i+1}</td>
                <td class="code">{row['code']}</td>
                <td class="name">{row['name']}</td>
                <td><span class="badge" style="background:{_cat_color(row['category'])}">{row['category']}</span></td>
                <td>{row['industry']}</td>
                <td>{row['current_close']:.2f}</td>
                <td>{row['vol_ratio']:.1%}</td>
                <td>{row['drawdown_from_high']:.1f}%</td>
                <td>{row['recent_amplitude']:.1f}%</td>
                <td class="confirm" style="color:{_confirm_color(row['confirm_status'])}">{row['confirm_status']}</td>
                <td class="score">{row['score']}</td>
                <td class="reasons">{row['reasons']}</td>
            </tr>""")
    else:
        suff_rows.append('<tr><td colspan="12" style="text-align:center;color:#999;padding:30px;">AI\u677f\u5757\u6682\u65e0\u91cf\u7a92\u606f\u8fbe\u6807\u4e2a\u80a1</td></tr>')

    # 接近窒息
    near_rows = []
    for _, row in near_df.head(30).iterrows():
        near_rows.append(f"""
        <tr>
            <td class="code">{row['code']}</td>
            <td class="name">{row['name']}</td>
            <td><span class="badge" style="background:{_cat_color(row['category'])}">{row['category']}</span></td>
            <td>{row['industry']}</td>
            <td>{row['current_close']:.2f}</td>
            <td>{row['vol_ratio']:.1%}</td>
            <td>{row['drawdown']:.1f}%</td>
            <td>{row['change_5d']:+.1f}%</td>
            <td>{row['change_20d']:+.1f}%</td>
            <td style="color:{_status_color(row['status'])};font-weight:bold;">{row['status']}</td>
        </tr>""")

    # 强势上攻
    strong_rows = []
    for _, row in strong_df.head(15).iterrows():
        strong_rows.append(f"""
        <tr>
            <td class="code">{row['code']}</td>
            <td class="name">{row['name']}</td>
            <td><span class="badge" style="background:{_cat_color(row['category'])}">{row['category']}</span></td>
            <td>{row['industry']}</td>
            <td>{row['current_close']:.2f}</td>
            <td>{row['vol_ratio']:.1%}</td>
            <td>{row['drawdown']:.1f}%</td>
            <td>{row['change_20d']:+.1f}%</td>
        </tr>""")

    # 大市值一览
    big_rows = []
    for _, row in big_df.iterrows():
        ma20_flag = '<span style="color:#f44336;">\u7ad9\u4e0a</span>' if row['above_ma20'] else '<span style="color:#4caf50;">\u8dcc\u7834</span>'
        ma60_flag = '<span style="color:#f44336;">\u7ad9\u4e0a</span>' if row['above_ma60'] else '<span style="color:#4caf50;">\u8dcc\u7834</span>'
        big_rows.append(f"""
        <tr>
            <td class="code">{row['code']}</td>
            <td class="name">{row['name']}</td>
            <td><span class="badge" style="background:{_cat_color(row['category'])}">{row['category']}</span></td>
            <td>{row['industry']}</td>
            <td>{row['current_close']:.2f}</td>
            <td>{row['vol_ratio']:.1%}</td>
            <td>{row['drawdown']:.1f}%</td>
            <td>{row['change_5d']:+.1f}%</td>
            <td>{row['change_20d']:+.1f}%</td>
            <td>{ma20_flag}</td>
            <td>{ma60_flag}</td>
            <td style="color:{_status_color(row['status'])};font-weight:bold;">{row['status']}</td>
        </tr>""")

    # 趋势状态分布
    status_stats = trend_df.groupby(['category', 'status']).agg(
        count=('code', 'count'),
        avg_drawdown=('drawdown', 'mean'),
        avg_vol_ratio=('vol_ratio', 'mean'),
    ).sort_values('count', ascending=False)
    status_rows = []
    for (cat, status), row in status_stats.iterrows():
        status_rows.append(f"""
        <tr>
            <td><span class="badge" style="background:{_cat_color(cat)}">{cat}</span></td>
            <td>{status}</td>
            <td>{row['count']}</td>
            <td>{row['avg_drawdown']:.1f}%</td>
            <td>{row['avg_vol_ratio']:.1%}</td>
        </tr>""")

    suff_count = len(results_df) if len(results_df) > 0 else 0
    hw_count = len(trend_df[trend_df['category'] == 'AI\u786c\u4ef6'])
    sw_count = len(trend_df[trend_df['category'] == 'AI\u8f6f\u4ef6'])
    strong_count = len(trend_df[trend_df['status'] == '\u5f3a\u52bf\u4e0a\u653b'])

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AI\u786c\u4ef6&\u8f6f\u4ef6\u91cf\u7a92\u606f\u5206\u6790 - {latest_date}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Microsoft YaHei', 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; padding: 20px; }}
h1 {{ color: #7b1fa2; margin-bottom: 5px; }}
h2 {{ color: #333; margin: 25px 0 10px; font-size: 18px; border-left: 4px solid #7b1fa2; padding-left: 10px; }}
.subtitle {{ color: #666; margin-bottom: 20px; font-size: 14px; }}
.summary {{ display: flex; gap: 15px; margin-bottom: 20px; flex-wrap: wrap; }}
.summary-card {{ background: white; padding: 15px 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; min-width: 140px; }}
.summary-card .label {{ font-size: 12px; color: #999; }}
.summary-card .value {{ font-size: 28px; font-weight: bold; color: #7b1fa2; }}
.summary-card.hardware .value {{ color: #e91e63; }}
.summary-card.software .value {{ color: #9c27b0; }}
table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; font-size: 13px; }}
th {{ background: #7b1fa2; color: white; padding: 10px 8px; text-align: left; font-weight: 500; white-space: nowrap; }}
td {{ padding: 7px 8px; border-bottom: 1px solid #eee; }}
tr:hover {{ background: #faf5ff; }}
.code {{ font-family: monospace; font-weight: bold; }}
.name {{ font-weight: bold; }}
.confirm {{ font-weight: bold; white-space: nowrap; }}
.score {{ font-weight: bold; color: #7b1fa2; text-align: center; }}
.reasons {{ font-size: 12px; color: #666; max-width: 280px; }}
.badge {{ color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: bold; }}
.note {{ background: #f3e5f5; border: 1px solid #ce93d8; padding: 12px 15px; border-radius: 8px; margin-bottom: 20px; font-size: 13px; line-height: 1.8; }}
.note strong {{ color: #7b1fa2; }}
</style>
</head>
<body>
<h1>AI\u786c\u4ef6 & AI\u8f6f\u4ef6 \u91cf\u7a92\u606f\u5206\u6790</h1>
<p class="subtitle">\u626b\u63cf\u65e5\u671f: {latest_date} | AI\u677f\u5757\u80a1\u7968\u603b\u6570: {ai_total} | \u751f\u6210\u65f6\u95f4: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

<div class="summary">
    <div class="summary-card"><div class="label">AI\u677f\u5757\u603b\u6570</div><div class="value">{ai_total}</div></div>
    <div class="summary-card hardware"><div class="label">AI\u786c\u4ef6</div><div class="value">{hw_count}</div></div>
    <div class="summary-card software"><div class="label">AI\u8f6f\u4ef6</div><div class="value">{sw_count}</div></div>
    <div class="summary-card"><div class="label">\u91cf\u7a92\u606f\u8fbe\u6807</div><div class="value">{suff_count}</div></div>
    <div class="summary-card"><div class="label">\u63a5\u8fd1\u7a92\u606f(20-40%)</div><div class="value">{len(near_df)}</div></div>
    <div class="summary-card"><div class="label">\u5f3a\u52bf\u4e0a\u653b(\u672a\u56de\u8c03)</div><div class="value">{strong_count}</div></div>
</div>

<div class="note">
<strong>\u6838\u5fc3\u7ed3\u8bba</strong>\uff1aAI\u677f\u5757\u91cf\u7a92\u606f\u8fbe\u6807\u6570\u504f\u5c11\uff0c\u539f\u56e0\u662f\u5927\u90e8\u5206AI\u786c\u4ef6/\u8f6f\u4ef6\u80a1<strong>\u4ecd\u5728\u5f3a\u52bf\u4e0a\u653b\u6216\u521a\u653e\u91cf\u62c9\u5347</strong>\uff0c\u5c1a\u672a\u7ecf\u5386"\u653e\u91cf\u2192\u7f29\u91cf\u56de\u8e29\u2192\u6a2a\u76d8"\u7684\u5b8c\u6574\u8fc7\u7a0b\u3002
\u91cf\u7a92\u606f\u6218\u6cd5\u7684\u524d\u63d0\u662f"\u524d\u671f\u653e\u91cf\u62c9\u5b8c\u4e00\u6ce2\u540e\u7f29\u91cf\u56de\u8e29"\uff0cAI\u7968\u76ee\u524d\u591a\u5904\u4e8e\u8d8b\u52bf\u4e2d\u6bb5\uff0c<strong>\u6ca1\u56de\u8c03\u591f = \u6ca1\u91cf\u7a92\u606f\u673a\u4f1a</strong>\u3002
<br><br>
<strong>\u7b56\u7565\u5efa\u8bae</strong>\uff1a\u2460 \u91cf\u7a92\u606f\u8fbe\u6807\u7684\u7968\u6309\u65b9\u6cd5\u8bba\u4e09\u91cd\u786e\u8ba4\u8d70\u6d41\u7a0b\uff1b\u2461 \u63a5\u8fd1\u7a92\u606f\u7684\u7968\uff08\u91cf\u6bd420-40%\uff09\u653e\u5165\u81ea\u9009\u91cd\u70b9\u8ddf\u8e2a\uff0c\u7b49\u91cf\u80fd\u7ee7\u7eed\u8427\u7f29\u523020%\u4ee5\u4e0b+\u51fa\u73b0\u6536\u7ea2\u786e\u8ba4\uff1b\u2462 \u5f3a\u52bf\u4e0a\u653b\u7684\u7968\u4e0d\u662f\u91cf\u7a92\u606f\u7684\u83dc\uff0c\u4f46\u53ef\u5173\u6ce8\u5176\u4f55\u65f6\u89c1\u9876\u56de\u843d\uff0c\u56de\u843d\u7f29\u91cf\u540e\u53ef\u80fd\u6210\u4e3a\u4e0b\u4e00\u6279\u91cf\u7a92\u606f\u6807\u7684\u3002
</div>

<h2>\u4e00\u3001\u91cf\u7a92\u606f\u8fbe\u6807\u4e2a\u80a1\uff08\u6309\u8bc4\u5206\u6392\u5e8f\uff09</h2>
<table>
<thead><tr><th>#</th><th>\u4ee3\u7801</th><th>\u540d\u79f0</th><th>\u5206\u7c7b</th><th>\u884c\u4e1a</th><th>\u6536\u76d8\u4ef7</th><th>\u91cf\u6bd4</th><th>\u56de\u64a4</th><th>5\u65e5\u632f\u5e45</th><th>\u786e\u8ba4\u72b6\u6001</th><th>\u8bc4\u5206</th><th>\u7406\u7531</th></tr></thead>
<tbody>
{''.join(suff_rows)}
</tbody>
</table>

<h2>\u4e8c\u3001AI\u677f\u5757\u8d8b\u52bf\u72b6\u6001\u5206\u5e03</h2>
<table>
<thead><tr><th>\u5206\u7c7b</th><th>\u8d8b\u52bf\u72b6\u6001</th><th>\u6570\u91cf</th><th>\u5e73\u5747\u56de\u64a4</th><th>\u5e73\u5747\u91cf\u6bd4</th></tr></thead>
<tbody>
{''.join(status_rows)}
</tbody>
</table>

<h2>\u4e09\u3001\u63a5\u8fd1\u91cf\u7a92\u606f\u7684AI\u80a1\uff08\u91cf\u6bd420%-40%\uff0c\u7f29\u91cf\u56de\u8c03\u4e2d\uff0c\u91cd\u70b9\u8ddf\u8e2a\uff09</h2>
<table>
<thead><tr><th>\u4ee3\u7801</th><th>\u540d\u79f0</th><th>\u5206\u7c7b</th><th>\u884c\u4e1a</th><th>\u6536\u76d8\u4ef7</th><th>\u91cf\u6bd4</th><th>\u56de\u64a4</th><th>5\u65e5\u6da8\u8dcc</th><th>20\u65e5\u6da8\u8dcc</th><th>\u72b6\u6001</th></tr></thead>
<tbody>
{''.join(near_rows)}
</tbody>
</table>

<h2>\u56db\u3001\u4ecd\u5728\u5f3a\u52bf\u4e0a\u653b\u7684AI\u80a1\uff08\u6ca1\u56de\u8c03\uff0c\u6682\u65e0\u91cf\u7a92\u606f\u673a\u4f1a\uff09</h2>
<table>
<thead><tr><th>\u4ee3\u7801</th><th>\u540d\u79f0</th><th>\u5206\u7c7b</th><th>\u884c\u4e1a</th><th>\u6536\u76d8\u4ef7</th><th>\u91cf\u6bd4</th><th>\u56de\u64a4</th><th>20\u65e5\u6da8\u5e45</th></tr></thead>
<tbody>
{''.join(strong_rows)}
</tbody>
</table>

<h2>\u4e94\u3001\u5927\u5e02\u503cAI\u80a1\u8d8b\u52bf\u4e00\u89c8\uff08\u5e02\u503c\u524d30\uff09</h2>
<table>
<thead><tr><th>\u4ee3\u7801</th><th>\u540d\u79f0</th><th>\u5206\u7c7b</th><th>\u884c\u4e1a</th><th>\u6536\u76d8\u4ef7</th><th>\u91cf\u6bd4</th><th>\u56de\u64a4</th><th>5\u65e5\u6da8\u8dcc</th><th>20\u65e5\u6da8\u8dcc</th><th>MA20</th><th>MA60</th><th>\u72b6\u6001</th></tr></thead>
<tbody>
{''.join(big_rows)}
</tbody>
</table>

</body>
</html>"""

    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logging.info(f"HTML\u62a5\u544a\u5df2\u751f\u6210: {output_path}")
    return output_path
