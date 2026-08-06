#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填行业板块资金流向历史数据
================================
用途：回填指定日期的行业资金流向数据（cn_stock_fund_flow_industry 表）
原理：通过 push2his.eastmoney.com 的历史日线资金流API获取单一板块的历史数据

使用方法：
    python -m script.backfill_industry_fund_flow 2026-07-14

说明：
    - 今日数据：直接从 push2his 日线提取
    - 3日/5日净额：从日线数据滚动求和
    - stock_name（主力净流入最大股）：push2his不支持，填NULL
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, date
import requests
import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 添加项目根路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from instock.core import tablestructure as tbs
from instock.lib import database as mdb

# 禁用SSL验证警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== 配置 ====================
PUSH2HIS_BASE = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
CLIST_BASE = "https://push2.eastmoney.com/api/qt/clist/get"
TARGET_TABLE = "cn_stock_fund_flow_industry"
REQUEST_DELAY = 0.5  # 每次请求间隔（秒）
BATCH_SIZE = 50      # 每N个板块打印一次进度


def create_session():
    """创建带默认headers的requests session"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://data.eastmoney.com/',
        'Accept': '*/*',
    })
    session.verify = False
    return session


def get_all_industry_bk_codes(session):
    """
    从 push2 CLIST API 获取所有行业板块的 BK 代码和名称
    
    返回:
        list of dict: [{'bk_code': 'BK0475', 'name': '银行'}, ...]
    """
    url = (
        f"{CLIST_BASE}?pn=1&pz=500&po=1&np=1"
        f"&ut=bd1d9ddb04089700cf9c27f6f7426281"
        f"&fltt=2&invt=2&fid=f3"
        f"&fs=m:90+t:2"  # 行业板块
        f"&fields=f12,f14"
    )
    
    try:
        resp = session.get(url, timeout=15)
        data = resp.json()
        if data.get('data') and data['data'].get('diff'):
            items = data['data']['diff']
            sectors = []
            for item in items:
                bk_code = item.get('f12', '')
                name = item.get('f14', '')
                if bk_code and name:
                    sectors.append({'bk_code': bk_code, 'name': name})
            logging.info(f"✅ 获取到 {len(sectors)} 个行业板块")
            return sectors
        else:
            logging.error(f"❌ CLIST API 返回数据异常: {data.get('message', '无data')}")
            return []
    except Exception as e:
        logging.error(f"❌ 获取BK代码列表失败: {e}")
        return []


def fetch_sector_history(session, bk_code, lmt=10):
    """
    从 push2his 获取单个板块的历史资金流日线数据
    
    参数:
        session: requests session
        bk_code: BK代码（如 'BK0475'）
        lmt: 返回最近N条记录
    
    返回:
        list of dict: 解析后的日线数据，按日期升序排列
    """
    url = (
        f"{PUSH2HIS_BASE}?lmt={lmt}&klt=101"
        f"&secid=90.{bk_code}"
        f"&fields1=f1,f2,f3,f7"
        f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63"
    )
    
    try:
        resp = session.get(url, timeout=15)
        data = resp.json()
        
        if data.get('data') and data['data'].get('klines'):
            klines = data['data']['klines']
            records = []
            for line in klines:
                if not line:
                    continue
                parts = line.split(',')
                if len(parts) < 14:
                    continue
                records.append({
                    'date': parts[0],                    # f51: 日期
                    'fund_amount': float(parts[1]),       # f52: 主力净流入
                    'fund_amount_small': float(parts[2]),  # f53: 小单净流入
                    'fund_amount_medium': float(parts[3]), # f54: 中单净流入
                    'fund_amount_large': float(parts[4]),  # f55: 大单净流入
                    'fund_amount_super': float(parts[5]),  # f56: 超大单净流入
                    'fund_rate': float(parts[6]),          # f57: 主力净流入占比
                    'fund_rate_small': float(parts[7]),    # f58: 小单净流入占比
                    'fund_rate_medium': float(parts[8]),   # f59: 中单净流入占比
                    'fund_rate_large': float(parts[9]),    # f60: 大单净流入占比
                    'fund_rate_super': float(parts[10]),   # f61: 超大单净流入占比
                    'close': float(parts[11]),             # f62: 收盘指数
                    'change_rate': float(parts[12]),       # f63: 涨跌幅
                })
            return records
        else:
            return None
    except Exception as e:
        logging.warning(f"  ⚠️ {bk_code}: 请求失败 - {e}")
        return None


def extract_target_data(records, target_date_str, bk_name):
    """
    从日线记录中提取目标日期的数据，并计算5日/10日汇总
    
    参数:
        records: 日线记录列表（按时间排序）
        target_date_str: 目标日期字符串 'YYYY-MM-DD'
        bk_name: 板块名称
    
    返回:
        dict: 包含今日/5日/10日所有字段的数据字典，如果当天无数据返回None
    """
    if not records:
        return None
    
    # 按日期升序排序
    records = sorted(records, key=lambda x: x['date'])
    
    # 找到目标日期的索引
    target_idx = None
    for i, r in enumerate(records):
        if r['date'] == target_date_str:
            target_idx = i
            break
    
    if target_idx is None:
        return None
    
    today = records[target_idx]
    
    # --- 计算5日汇总（目标日 + 前4个交易日） ---
    d5_range = records[max(0, target_idx - 4):target_idx + 1]
    d5_result = _calc_period_aggregate(d5_range)
    
    # --- 计算10日汇总（目标日 + 前9个交易日） ---
    d10_range = records[max(0, target_idx - 9):target_idx + 1]
    d10_result = _calc_period_aggregate(d10_range)
    
    # 组装完整记录
    record = {
        'date': target_date_str,
        'name': bk_name,
        
        # 今日
        'change_rate': today['change_rate'],
        'fund_amount': int(today['fund_amount']),
        'fund_rate': today['fund_rate'],
        'fund_amount_super': int(today['fund_amount_super']),
        'fund_rate_super': today['fund_rate_super'],
        'fund_amount_large': int(today['fund_amount_large']),
        'fund_rate_large': today['fund_rate_large'],
        'fund_amount_medium': int(today['fund_amount_medium']),
        'fund_rate_medium': today['fund_rate_medium'],
        'fund_amount_small': int(today['fund_amount_small']),
        'fund_rate_small': today['fund_rate_small'],
        'stock_name': None,  # push2his 不提供
        
        # 5日
        'change_rate_5': d5_result['change_rate'],
        'fund_amount_5': int(d5_result['fund_amount']),
        'fund_rate_5': d5_result['fund_rate'],
        'fund_amount_super_5': int(d5_result['fund_amount_super']),
        'fund_rate_super_5': d5_result['fund_rate_super'],
        'fund_amount_large_5': int(d5_result['fund_amount_large']),
        'fund_rate_large_5': d5_result['fund_rate_large'],
        'fund_amount_medium_5': int(d5_result['fund_amount_medium']),
        'fund_rate_medium_5': d5_result['fund_rate_medium'],
        'fund_amount_small_5': int(d5_result['fund_amount_small']),
        'fund_rate_small_5': d5_result['fund_rate_small'],
        'stock_name_5': None,
        
        # 10日
        'change_rate_10': d10_result['change_rate'],
        'fund_amount_10': int(d10_result['fund_amount']),
        'fund_rate_10': d10_result['fund_rate'],
        'fund_amount_super_10': int(d10_result['fund_amount_super']),
        'fund_rate_super_10': d10_result['fund_rate_super'],
        'fund_amount_large_10': int(d10_result['fund_amount_large']),
        'fund_rate_large_10': d10_result['fund_rate_large'],
        'fund_amount_medium_10': int(d10_result['fund_amount_medium']),
        'fund_rate_medium_10': d10_result['fund_rate_medium'],
        'fund_amount_small_10': int(d10_result['fund_amount_small']),
        'fund_rate_small_10': d10_result['fund_rate_small'],
        'stock_name_10': None,
    }
    return record


def _calc_period_aggregate(period_records):
    """
    计算多日汇总数据
    
    金额 = 各日金额之和
    涨跌幅 = 最后一天的涨跌幅（近似）
    占比 = 净额合计 / 当日占比合计的加权... 由于没有成交额，
           这里用各日净额加权近似：sum(net_flow * rate) / sum(net_flow)
           如果净额为0，则直接用平均占比
    """
    result = {}
    amount_fields = ['fund_amount', 'fund_amount_super', 'fund_amount_large', 
                     'fund_amount_medium', 'fund_amount_small']
    rate_fields = ['fund_rate', 'fund_rate_super', 'fund_rate_large',
                   'fund_rate_medium', 'fund_rate_small']
    
    for f in amount_fields:
        result[f] = sum(r[f] for r in period_records)
    
    for f, af in zip(rate_fields, amount_fields):
        total_amount = abs(result[af])
        if total_amount > 0:
            # 用各日净额绝对值加权平均占比
            weighted_sum = sum(abs(r[af]) * r[f] for r in period_records)
            result[f] = round(weighted_sum / total_amount, 4)
        else:
            # 净额为零时取简单平均
            result[f] = round(sum(r[f] for r in period_records) / len(period_records), 4)
    
    # 涨跌幅：取最后一天（目标日）的涨跌幅
    result['change_rate'] = period_records[-1]['change_rate'] if period_records else 0
    
    return result


def main():
    parser = argparse.ArgumentParser(description='回填行业板块资金流向历史数据')
    parser.add_argument('date', type=str, help='目标日期，格式: YYYY-MM-DD')
    parser.add_argument('--test', action='store_true', help='测试模式：只处理前5个板块')
    parser.add_argument('--skip-check', action='store_true', help='跳过已存在数据检查')
    args = parser.parse_args()
    
    target_date = args.date
    test_mode = args.test
    
    logging.info("=" * 60)
    logging.info(f"行业资金流向回填任务 - 目标日期: {target_date}")
    if test_mode:
        logging.info("⚠️ 测试模式：只处理前5个板块")
    logging.info("=" * 60)
    
    # 检查是否已有当天数据
    if not args.skip_check and not test_mode:
        try:
            check_sql = f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE} WHERE date = '{target_date}'"
            result = mdb.executeSqlFetch(check_sql)
            existing = result[0][0] if result else 0
            if existing > 0:
                logging.info(f"⚠️ 表 {TARGET_TABLE} 中 {target_date} 已有 {existing} 条记录，跳过。")
                logging.info("   如需强制回填请加 --skip-check 参数。")
                return
        except Exception as e:
            logging.warning(f"检查已有数据失败（表可能不存在）: {e}")
    
    session = create_session()
    
    # 步骤1: 获取所有行业BK代码
    logging.info("📋 步骤1: 获取行业板块列表...")
    sectors = get_all_industry_bk_codes(session)
    if not sectors:
        logging.error("❌ 无法获取行业板块列表，退出。")
        return
    
    if test_mode:
        sectors = sectors[:5]
    
    # 步骤2: 逐个获取历史数据
    logging.info(f"📊 步骤2: 获取 {len(sectors)} 个板块的历史资金流数据...")
    
    all_records = []
    success_count = 0
    no_data_count = 0
    fail_count = 0
    
    for i, sector in enumerate(sectors):
        bk_code = sector['bk_code']
        bk_name = sector['name']
        
        # 获取最近20天数据（足够覆盖10日计算）
        records = fetch_sector_history(session, bk_code, lmt=20)
        
        if records is None:
            fail_count += 1
        else:
            target = extract_target_data(records, target_date, bk_name)
            if target:
                all_records.append(target)
                success_count += 1
            else:
                no_data_count += 1
        
        # 进度显示
        if (i + 1) % BATCH_SIZE == 0 or i == len(sectors) - 1:
            logging.info(f"  进度: {i+1}/{len(sectors)} "
                        f"(成功:{success_count}, 无数据:{no_data_count}, 失败:{fail_count})")
        
        # 请求间隔
        time.sleep(REQUEST_DELAY)
    
    logging.info(f"\n📈 汇总: 总计{len(sectors)}板块, "
                f"成功{success_count}, 无07-14数据{no_data_count}, 请求失败{fail_count}")
    
    if not all_records:
        logging.warning("⚠️ 没有获取到任何数据，退出。")
        return
    
    # 步骤3: 构建DataFrame并保存
    logging.info(f"\n💾 步骤3: 保存 {len(all_records)} 条记录到数据库...")
    
    df = pd.DataFrame(all_records)
    df = df[tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns'].keys()]
    
    # 检查目标表是否存在
    try:
        if mdb.checkTableIsExist(TARGET_TABLE):
            cols_type = None
        else:
            logging.info(f"  ⚠️ 表 {TARGET_TABLE} 不存在，自动创建")
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns'])
    except Exception:
        cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_FUND_FLOW_INDUSTRY['columns'])
    
    # 删除已存在的同日数据（避免主键冲突）
    try:
        mdb.executeSql(f"DELETE FROM {TARGET_TABLE} WHERE date = '{target_date}'")
    except Exception as e:
        logging.warning(f"  清理旧数据失败: {e}")
    
    # 插入新数据
    mdb.insert_db_from_df(
        df, TARGET_TABLE, cols_type, False, "`date`,`name`"
    )
    
    logging.info(f"\n✅ 回填完成！")
    logging.info(f"   目标表: {TARGET_TABLE}")
    logging.info(f"   目标日期: {target_date}")
    logging.info(f"   成功写入: {len(all_records)} 条记录")
    logging.info(f"   无数据板块: {no_data_count} 个")
    logging.info(f"   请求失败: {fail_count} 个")


if __name__ == '__main__':
    main()
