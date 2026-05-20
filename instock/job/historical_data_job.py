#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
历史数据批量抓取模块
=====================

功能说明：
本模块用于从东方财富网抓取真正的历史数据（非实时快照）。
通过逐只股票获取历史K线数据，然后按日期聚合生成每日市场快照。

与execute_daily_job.py的区别：
- execute_daily_job.py: 获取当前实时数据，标记为指定日期（不准确）
- historical_data_job.py: 获取指定日期的真实历史数据（准确）

数据来源：
- 东方财富网历史K线API
- http://push2his.eastmoney.com/api/qt/stock/kline/get

使用示例：
    # 抓取单日历史数据
    python instock/job/historical_data_job.py 2024-01-01
    
    # 抓取日期区间
    python instock/job/historical_data_job.py 2024-01-01 2024-01-31
    
    # 测试模式（只抓前30只股票）
    python instock/job/historical_data_job.py 2024-01-01 --test
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import datetime  # 日期时间处理
import sys  # 系统操作
import os  # 路径操作
import pandas as pd  # 数据处理
from typing import List, Dict, Optional
import time  # 时间控制
import random  # 随机数生成（用于延迟）
import json  # JSON处理
from instock.config.delay_manager import sleep_with_delay

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
import instock.core.stockfetch as stf  # 数据抓取模块
from instock.core.crawling.stock_hist_em import stock_zh_a_hist, code_id_map_em
from instock.lib.trade_time import is_trade_date

__author__ = 'AI Assistant'
__date__ = '2026/05/04'


# ==================== 核心函数 ====================

def get_stock_list_for_date(date: datetime.date, sample_size: int = None) -> pd.DataFrame:
    """
    获取指定日期存在的股票列表
    
    参数：
        date: 目标日期
        sample_size: 采样数量（None表示不限制，获取全部；默认None）
        
    返回：
        DataFrame包含(code, name)列
    """
    try:
        # 方法1: 尝试从数据库中获取该日期已存在的股票列表
        table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        
        if mdb.checkTableIsExist(table_name):
            # 根据 sample_size 构建 SQL
            if sample_size is None:
                # 不限制数量，获取全部
                sql = f"SELECT `code`, `name` FROM `{table_name}`"
            else:
                # 限制数量
                sql = f"SELECT `code`, `name` FROM `{table_name}` LIMIT {sample_size}"
            
            result_tuple = mdb.executeSqlFetch(sql)
            
            if result_tuple is not None and len(result_tuple) > 0:
                # 将tuple转换为DataFrame
                df = pd.DataFrame(result_tuple, columns=['code', 'name'])
                logging.info(f"从数据库获取到 {len(df)} 只股票列表")
                return df
        
        # 方法2: 如果数据库没有，使用硬编码的股票代码列表（快速测试）
        logging.info("数据库无数据，使用预设股票列表...")
        
        # 预设一些常见股票代码用于测试
        preset_stocks = [
            ('600519', '贵州茅台'),
            ('000858', '五粮液'),
            ('600036', '招商银行'),
            ('601318', '中国平安'),
            ('000333', '美的集团'),
            ('600276', '恒瑞医药'),
            ('000001', '平安银行'),
            ('600000', '浦发银行'),
            ('601166', '兴业银行'),
            ('600030', '中信证券'),
            ('000002', '万科A'),
            ('600887', '伊利股份'),
            ('601012', '隆基绿能'),
            ('002594', '比亚迪'),
            ('300750', '宁德时代'),
            ('600900', '长江电力'),
            ('601888', '中国中免'),
            ('600309', '万华化学'),
            ('000568', '泸州老窖'),
            ('601088', '中国神华'),
            ('600048', '保利发展'),
            ('002475', '立讯精密'),
            ('000651', '格力电器'),
            ('601398', '工商银行'),
            ('601939', '建设银行'),
            ('601288', '农业银行'),
            ('601988', '中国银行'),
            ('601668', '中国建筑'),
            ('601601', '中国太保'),
            ('601628', '中国人寿'),
            ('600031', '三一重工'),
            ('601899', '紫金矿业'),
        ]
        
        # 转换为DataFrame
        df_preset = pd.DataFrame(preset_stocks, columns=['code', 'name'])
        
        # 如果需要更多股票，补充一些随机代码
        if sample_size is not None and sample_size > len(df_preset):
            additional_codes = [
                (f'{600000 + i:06d}', f'测试股票{i}') for i in range(1, sample_size - len(df_preset) + 1)
            ]
            df_additional = pd.DataFrame(additional_codes, columns=['code', 'name'])
            df_preset = pd.concat([df_preset, df_additional], ignore_index=True)
        
        # 如果指定了 sample_size，只取需要的数量
        if sample_size is not None:
            result = df_preset.head(sample_size)
        else:
            result = df_preset
        
        logging.info(f"使用预设股票列表 {len(result)} 只")
        return result
        
    except Exception as e:
        logging.error(f"获取股票列表失败: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return pd.DataFrame()


def get_stock_name_from_cache(stock_code: str) -> str:
    """
    从股票代码映射缓存文件中获取股票名称（优先使用）
    
    参数：
        stock_code: 股票代码
        
    返回：
        股票名称，如果未找到则返回''
    """
    try:
        cache_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cache', 'stock_code_map.json')
        
        if not os.path.exists(cache_file):
            return ''
        
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        code_map = cache_data.get('code_map', {})
        
        if stock_code not in code_map:
            return ''
        
        stock_info = code_map[stock_code]
        
        # 【兼容处理】支持新旧两种格式
        if isinstance(stock_info, dict):
            # 新格式：对象结构
            return stock_info.get('name', '')
        else:
            # 旧格式：只是数字（market_id），没有名称信息
            return ''
            
    except Exception as e:
        logging.debug(f"从缓存获取股票名称失败: {e}")
        return ''


def fetch_single_stock_history(
    code: str, 
    name: str,
    target_date: datetime.date,
    lookback_days: int = 365
) -> Optional[Dict]:
    """
    获取单只股票在指定日期的历史数据
    
    参数：
        code: 股票代码
        name: 股票名称
        target_date: 目标日期
        lookback_days: 向前回溯天数（默认365天，确保有足够数据计算指标）
        
    返回：
        字典包含该股票在target_date的快照数据，如果失败返回None
    """
    try:
        # 计算开始日期（向前回溯）
        start_date = target_date - datetime.timedelta(days=lookback_days)
        
        # 格式化日期（YYYYMMDD格式）
        start_str = start_date.strftime('%Y%m%d')
        end_str = target_date.strftime('%Y%m%d')
        
        # 调用历史K线API
        df_hist = stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start_str,
            end_date=end_str,
            adjust="qfq"  # 前复权
        )
        
        # 检查是否获取到数据
        if df_hist is None or len(df_hist) == 0:
            logging.warning(f"{code} {name}: API返回空数据（可能股票已退市或代码无效）")
            return None
        
        # 筛选出目标日期的数据
        target_row = df_hist[df_hist['日期'] == target_date.strftime('%Y-%m-%d')]
        
        if len(target_row) == 0:
            logging.warning(f"{code} {name}: {target_date} 无交易数据（可能停牌或非交易日）")
            return None
        
        # 提取该日期的数据
        row = target_row.iloc[0]
        
        # 【优化】如果名称为空，从缓存中获取
        if not name:
            name = get_stock_name_from_cache(code)
        
        # 构造符合cn_stock_spot表结构的数据
        # 注意：历史K线API只提供基础行情数据，其他字段需要设置为None或默认值
        result = {
            # 基础字段（从K线API获取）
            'date': target_date.strftime('%Y-%m-%d'),
            'code': code,
            'name': name if name else '',
            'new_price': float(row.get('收盘', 0)),
            'change_rate': float(row.get('涨跌幅', 0)),
            'ups_downs': float(row.get('涨跌额', 0)),
            'volume': float(row.get('成交量', 0)),
            'deal_amount': float(row.get('成交额', 0)),
            'amplitude': float(row.get('振幅', 0)),
            'turnoverrate': float(row.get('换手率', 0)),
            'open_price': float(row.get('开盘', 0)),
            'high_price': float(row.get('最高', 0)),
            'low_price': float(row.get('最低', 0)),
            'pre_close_price': float(row.get('昨收', 0)) if '昨收' in row else 0,
            
            # 以下字段历史K线API不提供，设置为None
            'volume_ratio': None,  # 量比
            'speed_increase': None,  # 涨速
            'speed_increase_5': None,  # 5分钟涨跌
            'speed_increase_60': None,  # 60日涨跌幅
            'speed_increase_all': None,  # 年初至今涨跌幅
            'dtsyl': None,  # 市盈率动
            'pe9': None,  # 市盈率TTM
            'pe': None,  # 市盈率静
            'pbnewmrq': None,  # 市净率
            'basic_eps': None,  # 每股收益
            'bvps': None,  # 每股净资产
            'per_capital_reserve': None,  # 每股公积金
            'per_unassign_profit': None,  # 每股未分配利润
            'roe_weight': None,  # 加权净资产收益率
            'sale_gpr': None,  # 毛利率
            'debt_asset_ratio': None,  # 资产负债率
            'total_operate_income': None,  # 营业收入
            'toi_yoy_ratio': None,  # 营业收入同比增长
            'parent_netprofit': None,  # 归属净利润
            'netprofit_yoy_ratio': None,  # 归属净利润同比增长
            'report_date': None,  # 报告期
            'total_shares': None,  # 总股本
            'free_shares': None,  # 已流通股份
            'total_market_cap': None,  # 总市值
            'free_cap': None,  # 流通市值
            'industry': None,  # 所处行业
            'listing_date': None,  # 上市时间
        }
        
        return result
        
    except Exception as e:
        logging.warning(f"{code} {name}: 获取失败 - {type(e).__name__}: {str(e)[:150]}")
        return None


def save_historical_snapshot(date: datetime.date, test_mode: bool = False):
    """
    抓取并保存指定日期的历史数据快照
    
    参数：
        date: 目标日期
        test_mode: 测试模式（只抓取前30只股票）
    """
    try:
        # 步骤1: 检查是否为交易日
        if not is_trade_date(date):
            logging.info(f"{date} 不是交易日，跳过")
            return
        
        logging.info("")
        logging.info("=" * 80)
        logging.info(f"[{date}] 开始抓取历史数据快照...")
        
        # 步骤2: 获取股票列表
        # - 测试模式：只取30只股票（快速验证）
        # - 生产模式：取None表示获取全市场所有股票
        if test_mode:
            sample_size = 30
            logging.info(f"测试模式：只抓取前 {sample_size} 只股票")
        else:
            sample_size = None  # None 表示不限制数量，获取全部
            logging.info(f"生产模式：将抓取全市场所有股票")
        
        stock_list = get_stock_list_for_date(date, sample_size)
        
        if stock_list is None or len(stock_list) == 0:
            logging.error(f"无法获取股票列表，终止执行")
            return
        
        total_stocks = len(stock_list)
        logging.info(f"总共需要抓取 {total_stocks} 只股票的历史数据")
        
        # 步骤3: 【串行执行】逐只获取历史数据（避免反爬）
        results = []
        success_count = 0
        fail_count = 0
        
        logging.info(f"开始串行获取 {total_stocks} 只股票的历史数据...")
        
        for idx, (_, row) in enumerate(stock_list.iterrows(), 1):
            code = row['code']
            name = row['name']
            
            try:
                # 获取单只股票的历史数据
                result = fetch_single_stock_history(code, name, date)
                
                if result is not None:
                    results.append(result)
                    success_count += 1
                else:
                    fail_count += 1
                    logging.warning(f"{code} {name}: 无数据（可能停牌、退市或新股）")
                    
            except Exception as e:
                logging.warning(f"{code} {name}: 异常 - {type(e).__name__}: {str(e)[:100]}")
                fail_count += 1
                # 失败后额外延迟
                sleep_with_delay('retry')
                continue
            
            # 进度日志（每10只显示一次）
            if idx % 10 == 0 or idx == total_stocks:
                logging.info(f"进度: {idx}/{total_stocks} (成功:{success_count}, 失败:{fail_count})")
            
            # 添加随机延迟（从配置文件读取）
            sleep_with_delay('normal')
        
        # 步骤4: 转换为DataFrame
        if not results:
            logging.warning(f"未获取到任何有效数据")
            return
        
        df_result = pd.DataFrame(results)
        
        logging.info(f"成功构建 {len(df_result)} 条记录，共 {len(df_result.columns)} 个字段")
        
        # 步骤6: 保存到数据库
        table_name = tbs.TABLE_CN_STOCK_SPOT['name']
        
        # 【修复】先检查是否存在旧数据，然后删除并验证
        if mdb.checkTableIsExist(table_name):
            try:
                check_sql = f"SELECT COUNT(*) as cnt FROM `{table_name}` WHERE `date` = '{date}'"
                result_before = mdb.executeSqlFetch(check_sql)
                existing_count = result_before[0][0] if result_before else 0
                
                if existing_count > 0:
                    logging.warning(f"发现 {existing_count} 条 {date} 的旧数据，正在删除...")
                    del_sql = f"DELETE FROM `{table_name}` WHERE `date` = '{date}'"
                    mdb.executeSql(del_sql)
                    
                    # 验证删除是否成功
                    result_after = mdb.executeSqlFetch(check_sql)
                    remaining_count = result_after[0][0] if result_after else 0
                    
                    if remaining_count > 0:
                        logging.error(f"删除失败！仍有 {remaining_count} 条旧数据残留")
                        logging.error("提示：请手动清理数据库后重新运行脚本")
                        return  # 终止流程，避免数据混乱
                    
                    logging.info(f"成功删除 {existing_count} 条旧数据")
                else:
                    logging.info(f"没有 {date} 的旧数据，直接插入")
                    
            except Exception as e:
                logging.error(f"删除旧数据失败: {e}")
                logging.error("提示：请检查数据库连接和权限")
                raise  # 删除失败应该终止流程
        
        # 插入新数据
        cols_type = None
        if not mdb.checkTableIsExist(table_name):
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT['columns'])
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (历史K线数据)")
        logging.info(f"   目标日期: {date}")
        logging.info(f"   数据量: {len(df_result)}条记录")
        logging.info(f"   开始插入数据...")
        
        try:
            mdb.insert_db_from_df(df_result, table_name, cols_type, False, "`date`,`code`")
            logging.info(f"成功保存 {len(df_result)} 条记录到数据库")
        except Exception as e:
            logging.error(f"批量插入失败: {type(e).__name__}: {str(e)[:200]}")
            
            # 尝试逐条插入，记录每条的失败原因
            logging.warning("尝试逐条插入...")
            success_insert = 0
            fail_insert = 0
            fail_details = []
            
            for idx, (_, row) in enumerate(df_result.iterrows(), 1):
                try:
                    single_df = pd.DataFrame([row.to_dict()])
                    mdb.insert_db_from_df(single_df, table_name, cols_type, False, "`date`,`code`")
                    success_insert += 1
                except Exception as insert_err:
                    fail_insert += 1
                    code = row.get('code', 'Unknown')
                    name = row.get('name', 'Unknown')
                    fail_details.append(f"{code} {name}: {type(insert_err).__name__}")
                    
                    # 只记录前5个失败的详细信息
                    if len(fail_details) <= 5:
                        logging.warning(f"[{idx}/{len(df_result)}] {code} {name}: {str(insert_err)[:100]}")
            
            logging.info(f"逐条插入完成: 成功={success_insert}, 失败={fail_insert}")
            
            if fail_insert > 0:
                logging.warning(f"失败的股票示例: {'; '.join(fail_details[:3])}")
        
        logging.info(f"统计: 成功={success_count}, 失败={fail_count}, 总计={total_stocks}")
        
    except Exception as e:
        logging.error(f"save_historical_snapshot处理异常: {e}")
        import traceback
        logging.error(traceback.format_exc())


def main():
    """
    主函数：支持命令行参数
    """
    # 导入任务工具
    from instock.job.task_utils import log_task_start
    
    # 解析命令行参数（移除 --test 标记）
    test_mode = '--test' in sys.argv
    if test_mode:
        sys.argv = [arg for arg in sys.argv if arg != '--test']
    
    # 任务开始日志
    log_task_start("historical_data_fetch", "抓取股票历史K线数据并保存到数据库")
    
    # 调用run_with_args处理日期参数
    runt.run_with_args(lambda date: save_historical_snapshot(date, test_mode))


# ==================== 程序入口 ====================
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    运行示例：
        # 测试模式（只抓前30只股票）
        python historical_data_job.py 2024-01-01 --test
        
        # 正式模式（抓取所有股票）
        python historical_data_job.py 2024-01-01
        
        # 日期区间
        python historical_data_job.py 2024-01-01 2024-01-31
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    
    logging.info("##### 历史数据抓取任务启动 #####")
    main()
    logging.info("##### 历史数据抓取任务完成 #####")
