#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
收盘后数据任务模块（第二层）
===========================
这个模块负责抓取收盘后才发布的数据。

主要数据：
1. 大宗交易数据
2. 尾盘抢筹数据

为什么需要单独的收盘后任务？
- 这些数据延迟发布
- 通常在收盘后1-2小时才有
- 如果收盘前运行会没有数据
- 单独任务便于管理

什么是大宗交易？
- 单笔成交数量大、金额大的交易
- 不通过正常买卖盘撮合
- 通常有折价或溢价
- 可能是机构调仓、股东减持

什么是尾盘抢筹？
- 收盘前30分钟快速拉升
- 可能是拉升收盘价
- 或主力急于建仓

数据发布时间：
- 大宗交易：约17:00后发布
- 尾盘抢筹：收盘后即有

运行时机：
- 收盘后运行：17:30后
- 确保数据已发布

注意事项：
- 17:00前运行可能没有数据
- 不会报错，只是返回空数据
- 建议晚上运行或第二天运行
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import os.path  # 路径操作
import sys  # 系统操作

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
import instock.core.stockfetch as stf  # 数据抓取模块

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 1. 保存大宗交易数据 ====================

"""
保存大宗交易数据
什么是大宗交易？
- 单笔交易量大（通常几十万股以上）
- 成交金额大（通常几千万以上）
- 不通过正常买卖盘
- 买卖双方协商价格
大宗交易特点：
- 通常有折价（低于市价）
- 也可能溢价（高于市价）
- 不影响二级市场价格
- 但反映大资金动向
数据包含：
- 成交价格
- 成交数量
- 成交金额
- 买方席位
- 卖方席位
- 溢价率
分析要点：
- 折价率高：可能是股东减持，看跌
- 溢价买入：买方看好，看涨
- 成交量大：重要信号
- 机构对倒：可能有特殊意图
数据发布时间：
- 收盘后1-2小时
- 约17:00后
使用场景：
- 发现大资金动向
- 股东减持预警
- 机构调仓分析
"""
def save_after_close_stock_blocktrade_data(date):
    try:
        # 抓取大宗交易数据
        data = stf.fetch_stock_blocktrade_data(date)
        if data is None or len(data.index) == 0:
            # 可能还没有数据（时间太早）
            logging.info(f"大宗交易数据暂无：{date}（可能17:00后才有）")
            return

        table_name = tbs.TABLE_CN_STOCK_BLOCKTRADE['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_BLOCKTRADE['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存大宗交易数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_after_close_daily_job.save_stock_blocktrade_data处理异常：{e}")


# ==================== 2. 保存尾盘抢筹数据 ====================

"""
保存尾盘抢筹数据
什么是尾盘抢筹？
- 收盘前30分钟快速拉升
- 可能是拉升收盘价
- 也可能是主力急于建仓
分析要点：
- 拉升幅度：越大越强势
- 成交量：放量说明真实
- 后续走势：第二天是否高开
意义：
- 强势股标志
- 主力控盘能力强
- 第二天可能继续强势
使用场景：
- 发现强势股
- 短线交易机会
- 尾盘竞价参考
"""
def save_after_close_stock_chip_race_end_data(date):
    try:
        # 抓取尾盘抢筹数据
        data = stf.fetch_stock_chip_race_end(date)
        if data is None or len(data.index) == 0:
            return

        table_name = tbs.TABLE_CN_STOCK_CHIP_RACE_END['name']
        # 删除老数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_CHIP_RACE_END['columns'])

        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"保存尾盘抢筹数据成功：{len(data)}条")
    except Exception as e:
        logging.error(f"basic_data_after_close_daily_job.save_after_close_stock_chip_race_end_data：{e}")


# ==================== 主函数 ====================

"""
收盘后数据任务主函数
执行任务：
1. 大宗交易数据
2. 尾盘抢筹数据
运行时间：
- 建议：17:30后
- 确保数据已发布
注意：
- 如果17:00前运行，大宗交易数据可能为空
- 不会报错，只是没有数据
"""
def main():
    # 1. 大宗交易
    runt.run_with_args(save_after_close_stock_blocktrade_data)
    
    # 2. 尾盘抢筹
    runt.run_with_args(save_after_close_stock_chip_race_end_data)
    
    logging.info("收盘后数据任务执行完成")


# ==================== 程序入口 ====================
if __name__ == '__main__':
    """
    直接运行此脚本
    
    运行方式：
        python basic_data_after_close_daily_job.py
        
    最佳运行时间：
        - 17:30后
        - 或第二天运行前一天的数据
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
收盘后数据任务使用总结（给Python新手）
===========================================

1. 模块定位
   - 第二层：数据抓取层
   - 收盘后数据：延迟发布的数据
   - 2个主要任务

2. 大宗交易
   重要性：
   - 反映大资金动向
   - 可能预示股价走势
   
   分析方法：
   - 折价：可能看跌
   - 溢价：可能看涨
   - 频繁：需要关注

3. 尾盘抢筹
   意义：
   - 强势股标志
   - 主力控盘
   - 第二天可能强势
   
   应用：
   - 短线选股
   - 竞价参考

4. 运行时间
   - 17:00前：可能无数据
   - 17:30后：数据完整
   - 第二天：补充前一天

5. 使用建议
   - 定时任务：17:30运行
   - 或手动：第二天运行
   - 检查日志：确认数据
"""
