#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易日历单例模块
================
这个模块提供交易日历数据的单例缓存。

什么是交易日历？
- 记录了股市的所有交易日期
- 用于判断某一天是否是交易日
- 排除了周末和节假日

为什么使用单例？
- 交易日历数据量不大
- 整个程序只需要加载一次
- 所有模块共享使用，避免重复请求

数据结构：
- set集合，包含所有交易日的date对象
- 使用set的好处：查找速度快O(1)

使用示例：
    from datetime import date
    from instock.core.singleton_trade_date import stock_trade_date
    
    # 获取交易日历单例
    trade_dates = stock_trade_date().get_data()
    
    # 判断某天是否是交易日
    if date(2024, 1, 1) in trade_dates:
        print("是交易日")
"""

import logging
import instock.core.stockfetch as stf
from instock.lib.singleton_type import singleton_type

__author__ = 'myh '
__date__ = '2023/3/10 '


class stock_trade_date(metaclass=singleton_type):
    """
    交易日历数据单例类
    
    功能说明：
        - 从网络获取历史所有交易日数据
        - 使用单例模式，整个程序只加载一次
        - 存储为set集合，方便快速查询
        
    属性：
        data (set): 交易日期集合，每个元素是datetime.date对象
        
    使用场景：
        1. 判断今天是否是交易日
        2. 获取上一个/下一个交易日
        3. 计算交易日区间
        4. 智能识别交易日（跳过周末和节假日）
        
    为什么重要？
        - 很多操作依赖于交易日判断
        - 数据抓取：只在交易日抓取
        - 数据分析：只分析交易日数据
        - 策略回测：只在交易日触发
        
    数据更新：
        - 程序启动时从网络获取最新数据
        - 包含历史所有交易日
        - 足够应对日常使用
    """
    
    def __init__(self):
        """
        初始化交易日历单例
        
        执行流程：
            1. 调用stockfetch模块的交易日历抓取函数
            2. 获取历史所有交易日数据
            3. 转换为set集合存储
            
        异常处理：
            如果网络请求失败，记录错误日志
            self.data可能为None
        """
        try:
            # 调用抓取函数获取交易日历
            # 返回set类型：{date(2021,1,4), date(2021,1,5), ...}
            self.data = stf.fetch_stocks_trade_date()
        except Exception as e:
            # 记录错误日志
            logging.error(f"singleton.stock_trade_date处理异常：{e}")

    def get_data(self):
        """
        获取交易日历数据
        
        返回值：
            set: 交易日期集合
                - 每个元素是datetime.date对象
                - 包含历史所有交易日
                - 如果获取失败可能为None
                
        使用示例：
            from datetime import date
            
            # 获取交易日历
            trade_dates = stock_trade_date().get_data()
            
            # 检查今天是否是交易日
            today = date.today()
            if today in trade_dates:
                print("今天是交易日，可以交易")
            else:
                print("今天不是交易日")
                
            # 统计交易日数量
            print(f"共有{len(trade_dates)}个交易日")
            
            # 获取最早和最晚的交易日
            if trade_dates:
                earliest = min(trade_dates)
                latest = max(trade_dates)
                print(f"数据范围：{earliest} 到 {latest}")
        """
        return self.data


"""
===========================================
交易日历单例模块使用总结（给Python新手）
===========================================

1. 核心功能
   - 提供交易日历数据
   - 单例模式，只加载一次
   - 快速查询某天是否是交易日

2. 数据结构
   - set集合：无序、不重复
   - 元素：datetime.date对象
   - 查询速度：O(1)常数时间

3. 单例模式
   - 第一次调用：从网络加载数据
   - 后续调用：直接返回缓存数据
   - 节省时间和网络资源

4. 应用场景
   - trade_time模块：判断交易日
   - 数据抓取：确定抓取日期
   - 策略回测：确定回测日期
   - Web显示：显示最新交易日

5. 注意事项
   - 程序启动时自动加载
   - 数据可能为None（网络失败）
   - 使用前最好检查是否为None
   - 数据包含历史所有交易日

6. Python知识点
   - set：集合数据结构
   - in运算符：检查元素是否在集合中
   - metaclass：元类，实现单例模式
   - date对象：日期类型（不包含时间）
"""
