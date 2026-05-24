#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
单只股票历史数据抓取脚本
========================

功能说明：
本脚本用于抓取指定股票的某段时间的历史K线数据。
适合用于测试、调试或获取特定股票的历史数据。

使用示例：
    # 抓取贵州茅台2024年1月的数据（测试用，时间段短）
    python instock/job/fetch_single_stock_hist.py 600519 2024-01-01 2024-01-10
    
    # 抓取平安银行2024年第一季度的数据
    python instock/job/fetch_single_stock_hist.py 000001 2024-01-01 2024-03-31
    
    # 保存到CSV文件
    python instock/job/fetch_single_stock_hist.py 600519 2024-01-01 2024-01-10 --output data.csv
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import datetime  # 日期时间处理
import sys  # 系统操作
import os  # 路径操作
import pandas as pd  # 数据处理
import argparse  # 命令行参数解析

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
from instock.core.crawling.stock_hist_em import stock_zh_a_hist
from instock.core.crawling.kline_utils import KLINE_COLUMNS, apply_kline_columns
from instock.core.crawling.market_utils import get_market_id
import instock.lib.database as mdb  # 数据库操作
import instock.core.tablestructure as tbs  # 表结构定义
from sqlalchemy import DATE, VARCHAR, DECIMAL, BIGINT


__author__ = 'AI Assistant'
__date__ = '2026/05/04'


def fetch_single_stock_history_fast(stock_code: str, start_date: str, end_date: str, 
                                     period: str = "daily", adjust: str = "qfq") -> pd.DataFrame:
    """
    快速抓取单只股票的历史K线数据（无需获取全市场代码映射）
    
    参数：
        stock_code: 股票代码（如：600519）
        start_date: 开始日期（格式：YYYY-MM-DD）
        end_date: 结束日期（格式：YYYY-MM-DD）
        period: K线周期
            - 'daily': 日线
            - 'weekly': 周线
            - 'monthly': 月线
        adjust: 复权类型
            - 'qfq': 前复权（推荐）
            - 'hfq': 后复权
            - '': 不复权
            
    返回：
        DataFrame包含历史K线数据
    """
    try:
        logging.info(f"开始抓取股票 {stock_code} 的历史数据...")
        logging.info(f"日期范围：{start_date} 至 {end_date}")
        logging.info(f"K线周期：{period}")
        logging.info(f"复权类型：{adjust}")
        
        # 直接调用底层API，绕过 code_id_map_em()
        from instock.core.crawling.stock_hist_em import fetcher
        import math
        
        # 根据股票代码判断市场ID
        market_id = get_market_id(stock_code)
        secid = f"{market_id}.{stock_code}"
        
        logging.info(f"股票ID: {secid}")
        
        # 构造API参数
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        
        adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
        period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
        
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "klt": period_dict[period],
            "fqt": adjust_dict[adjust],
            "secid": secid,
            "beg": start_date.replace("-", ""),
            "end": end_date.replace("-", ""),
            "_": "1623766962675",
        }
        
        # 发送HTTP请求
        logging.info("正在请求API...")
        r = fetcher.make_request(url, params=params)
        data_json = r.json()
        
        # 检查是否有数据
        if not (data_json.get("data") and data_json["data"].get("klines")):
            logging.warning(f"未获取到股票 {stock_code} 的数据")
            return pd.DataFrame()
        
        stock_name = data_json["data"].get("name", stock_code)
        logging.info(f"股票名称: {stock_name}")
        
        # 解析数据
        klines = data_json["data"]["klines"]
        temp_df = pd.DataFrame([item.split(",") for item in klines])
        
        # 重命名列
        temp_df = apply_kline_columns(temp_df)
        
        # 转换数据类型
        temp_df["日期"] = pd.to_datetime(temp_df["日期"])
        for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", 
                    "振幅", "涨跌幅", "涨跌额", "换手率"]:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
        
        logging.info(f"成功获取 {len(temp_df)} 条K线数据")
        logging.info(f"数据列：{', '.join(temp_df.columns.tolist())}")
        
        return temp_df, stock_name
        
    except Exception as e:
        logging.error(f"抓取失败：{e}")
        import traceback
        logging.error(traceback.format_exc())
        return pd.DataFrame()


def save_to_csv(df: pd.DataFrame, output_file: str):
    """
    将数据保存为CSV文件
    
    参数：
        df: 数据DataFrame
        output_file: 输出文件路径
    """
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"数据已保存到：{output_file}")
    except Exception as e:
        logging.error(f"保存失败：{e}")


def save_to_database(df: pd.DataFrame, stock_code: str, stock_name: str, table_name: str = "cn_stock_spot"):
    """
    将数据保存到MySQL数据库（使用cn_stock_spot表）
    
    参数：
        df: 数据DataFrame
        stock_code: 股票代码
        stock_name: 股票名称
        table_name: 表名（默认：cn_stock_spot）
    """
    if df.empty:
        logging.warning("⚠️ 没有数据可保存")
        return False
    
    try:
        logging.info(f"正在保存到数据库表：{table_name}...")
        
        # 准备数据：添加股票代码和名称列
        df_save = df.copy()
        df_save['code'] = stock_code
        df_save['name'] = stock_name
        
        # 重命名列以匹配 cn_stock_spot 表字段
        column_mapping = {
            '日期': 'date',
            '开盘': 'open_price',
            '收盘': 'new_price',  # cn_stock_spot使用new_price作为收盘价
            '最高': 'high_price',
            '最低': 'low_price',
            '成交量': 'volume',
            '成交额': 'deal_amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_rate',
            '涨跌额': 'ups_downs',
            '换手率': 'turnoverrate'
        }
        df_save.rename(columns=column_mapping, inplace=True)
        
        # 确保日期格式正确
        df_save['date'] = pd.to_datetime(df_save['date']).dt.date
        
        # 选择 cn_stock_spot 表需要的核心字段
        columns_needed = ['date', 'code', 'name', 'open_price', 'new_price', 'high_price', 
                         'low_price', 'volume', 'deal_amount', 'amplitude', 
                         'change_rate', 'ups_downs', 'turnoverrate']
        df_save = df_save[columns_needed]
        
        # 检查表是否存在
        if not mdb.checkTableIsExist(table_name):
            logging.error(f"表 {table_name} 不存在，请先运行 basic_data_daily_job 创建表")
            return False
        
        # 删除已存在的数据（避免重复）
        for _, row in df_save.iterrows():
            delete_sql = f"DELETE FROM `{table_name}` WHERE `date`='{row['date']}' AND `code`='{row['code']}'"
            mdb.executeSql(delete_sql)
        
        # 获取表结构定义
        cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SPOT['columns'])
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (历史K线数据)")
        logging.info(f"   目标日期范围: {df_save['date'].min()} 至 {df_save['date'].max()}")
        logging.info(f"   数据量: {len(df_save)}条记录")
        logging.info(f"   开始插入数据...")
        
        # 插入数据
        
        insert_count = mdb.insert_db_from_df(
            df_save, 
            table_name, 
            cols_type, 
            write_index=False,
            primary_keys="`date`,`code`"
        )
        
        logging.info(f"成功保存 {len(df_save)} 条记录到数据库表 {table_name}")
        return True
        
    except Exception as e:
        logging.error(f"保存到数据库失败：{e}")
        import traceback
        logging.error(traceback.format_exc())
        return False


def display_summary(df: pd.DataFrame):
    """
    显示数据摘要信息
    
    参数：
        df: 数据DataFrame
    """
    if df.empty:
        logging.warning("没有数据可显示")
        return
    
    logging.info("=" * 60)
    logging.info("数据摘要")
    logging.info("=" * 60)
    logging.info(f"总记录数：{len(df)}")
    logging.info(f"日期范围：{df.iloc[0]['日期']} 至 {df.iloc[-1]['日期']}")
    logging.info(f"开盘价范围：{df['开盘'].min():.2f} - {df['开盘'].max():.2f}")
    logging.info(f"收盘价范围：{df['收盘'].min():.2f} - {df['收盘'].max():.2f}")
    logging.info(f"最高价：{df['最高'].max():.2f}")
    logging.info(f"最低价：{df['最低'].min():.2f}")
    logging.info(f"成交量范围：{int(df['成交量'].min()):,} - {int(df['成交量'].max()):,}")
    logging.info(f"成交额范围：{df['成交额'].min():,.2f} - {df['成交额'].max():,.2f}")
    logging.info("=" * 60)
    
    # 显示前5条和后5条数据
    logging.info("\n前5条数据：")
    print(df.head().to_string())
    
    if len(df) > 5:
        logging.info("\n后5条数据：")
        print(df.tail().to_string())


def main():
    """主函数"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='抓取单只股票的历史K线数据')
    parser.add_argument('stock_code', help='股票代码（如：600519）')
    parser.add_argument('start_date', help='开始日期（格式：YYYY-MM-DD）')
    parser.add_argument('end_date', help='结束日期（格式：YYYY-MM-DD）')
    parser.add_argument('--period', choices=['daily', 'weekly', 'monthly'], 
                       default='daily', help='K线周期（默认：daily）')
    parser.add_argument('--adjust', choices=['qfq', 'hfq', ''], 
                       default='qfq', help='复权类型（默认：qfq前复权）')
    parser.add_argument('--output', help='输出CSV文件路径（可选）')
    parser.add_argument('--db', action='store_true', help='保存到MySQL数据库（cn_stock_spot表）')
    parser.add_argument('--table', default='cn_stock_spot', help='数据库表名（默认：cn_stock_spot）')
    
    args = parser.parse_args()
    
    # 验证日期格式
    try:
        start_dt = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
        end_dt = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
        
        if start_dt > end_dt:
            logging.error("开始日期不能晚于结束日期")
            sys.exit(1)
        
        # 计算天数差，提醒用户
        days_diff = (end_dt - start_dt).days
        if days_diff > 365:
            logging.warning(f"注意：时间跨度为 {days_diff} 天，可能会产生较多请求")
            response = input("是否继续？(y/n): ")
            if response.lower() != 'y':
                logging.info("已取消")
                sys.exit(0)
                
    except ValueError:
        logging.error("日期格式错误，请使用 YYYY-MM-DD 格式")
        sys.exit(1)
    
    # 抓取数据（使用快速版本，无需获取全市场代码映射）
    df, stock_name = fetch_single_stock_history_fast(
        stock_code=args.stock_code,
        start_date=args.start_date,
        end_date=args.end_date,
        period=args.period,
        adjust=args.adjust
    )
    
    # 如果获取到数据，显示摘要并保存
    if not df.empty:
        display_summary(df)
        
        # 保存到CSV文件
        if args.output:
            save_to_csv(df, args.output)
        
        # 保存到数据库
        if args.db:
            success = save_to_database(df, args.stock_code, stock_name, args.table)
            if success:
                logging.info(f"\n数据已保存到数据库表：{args.table}")
            else:
                logging.error(f"\n数据库保存失败")
        
        logging.info("\n任务完成！")
    else:
        logging.error("\n未获取到数据")
        sys.exit(1)


if __name__ == "__main__":
    main()
