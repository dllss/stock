#!/usr/local/bin/python
# -*- coding: utf-8 -*-
"""
任务运行模板模块（第九层 - 任务调度层）
========================================

模块功能：
---------
为所有 job 任务模块提供统一的日期参数解析和任务调度能力。
简化了任务的命令行参数处理，支持三种灵活的运行模式。

核心职责：
1. 解析命令行参数
2. 生成日期列表
3. 调度任务执行
4. 并发管理和错误处理

支持的三种运行模式：
-------------------

模式1：日期区间批量执行（2个命令行参数）
    python xxx.py 2024-01-01 2024-01-31
    功能：遍历区间内每个交易日，用线程池并发执行
    应用场景：批量补数据、回测验证、历史数据处理

模式2：指定日期列表（1个命令行参数，逗号分隔）
    python xxx.py 2024-01-01,2024-01-15,2024-01-30
    功能：只执行指定日期的任务
    应用场景：补缺失数据、特定日期处理

模式3：当日执行（无命令行参数）
    python xxx.py
    功能：自动获取最近交易日，根据函数名自动分发
    应用场景：日常定时任务、cron任务

自动分发规则（模式3）：
- save_nph_xxx：传入 run_date_nph（盘后日期），before=False
- save_after_close_xxx：传入 run_date（收盘日期）
- 其他函数：传入 run_date_nph

并发控制：
- 区间模式：线程池+2秒间隔（避免API限流）
- 列表模式：线程池执行
- 单日模式：单线程执行

使用示例：
--------
# 示例1：批量处理数据
def prepare(date):
    # 处理日期的任务
    fetch_data(date)

# 运行方式：
# python xxx.py 2024-01-01 2024-01-31  # 批量
# python xxx.py 2024-01-01             # 单日
# python xxx.py                        # 自动

实战应用：
--------
execute_daily_job.py：
    python execute_daily_job.py          # 运行今天的所有任务
    python execute_daily_job.py 2024-01-01 2024-01-31  # 补历史数据

indicators_data_daily_job.py：
    python indicators_data_daily_job.py  # 今天的指标计算
    python indicators_data_daily_job.py 2024-01-01,2024-01-15  # 补特定日期
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import datetime  # 日期时间处理
import concurrent.futures  # 多线程并发
import sys  # 系统参数
import time  # 延时处理

# ==================== 导入项目模块 ====================
import instock.lib.trade_time as trd  # 交易时间工具

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 通用任务调度函数 ====================

def run_with_args(run_fun, *args):
    """
    通用任务调度函数 - 解析命令行参数并执行任务
    
    参数说明：
    ---------
    run_fun (callable): 要执行的任务函数
        - 函数签名：run_fun(date, *args) -> None
        - date：datetime.date 对象
        - *args：额外参数
        
    *args: 传递给 run_fun 的额外参数
        - 例如：strategy配置、参数字典等
        
    执行方式自动判断（根据命令行参数）：
    ------------------------------------
    
    方式1：区间批量执行（len(sys.argv) == 3）
        命令行：python xxx.py START_DATE END_DATE
        示例：python xxx.py 2024-01-01 2024-01-31
        处理过程：
        1. 解析START_DATE和END_DATE
        2. 创建线程池
        3. 遍历区间内每一天
        4. 检查是否为交易日（使用 trd.is_trade_date()）
        5. 如果是交易日，提交任务到线程池
        6. 任务间隔2秒（防止API被限流）
        7. 等待所有任务完成
        
    方式2：指定日期列表（len(sys.argv) == 2）
        命令行：python xxx.py DATE1,DATE2,DATE3
        示例：python xxx.py 2024-01-01,2024-01-15,2024-01-30
        处理过程：
        1. 按逗号分割参数
        2. 创建线程池
        3. 遍历每个日期
        4. 检查是否为交易日
        5. 如果是交易日，提交任务到线程池
        6. 任务间隔2秒
        
    方式3：当日执行（len(sys.argv) == 1）
        命令行：python xxx.py
        处理过程：
        1. 获取最近交易日（两个值）
           - run_date：最近的交易日（用于收盘后数据）
           - run_date_nph：盘后日期（用于实时数据）
        2. 根据函数名前缀自动分发：
           - save_nph_xxx：传入run_date_nph，before=False
           - save_after_close_xxx：传入run_date
           - 其他：传入run_date_nph
    
    工作流程图：
    ----------
    输入命令行参数
         ↓
    判断参数个数
         ↓
    ┌────┬─────┬──────┐
    ↓    ↓     ↓      ↓
    3个  2个   1个    其他(默认)
    区间 列表  当日   错误
    
    并发控制：
    --------
    - ThreadPoolExecutor：并发线程池
    - 线程数：自动（CPU核数相关）
    - 间隔：2秒/任务（区间模式）
    - 目的：避免请求过快被服务器限流
    
    异常处理：
    --------
    - 捕获所有异常
    - 记录到日志
    - 继续执行下一个任务（不中断）
    
    使用示例：
    --------
    # 示例1：单个日期
    from instock.job.execute_daily_job import prepare
    from instock.lib.run_template import run_with_args
    run_with_args(prepare)
    
    # 示例2：日期区间
    # python script.py 2024-01-01 2024-01-31
    
    # 示例3：指定日期
    # python script.py 2024-01-01,2024-01-10,2024-01-20
    """
    if len(sys.argv) == 3:
        # ==================== 模式1：日期区间批量执行 ====================
        # python xxx.py START_DATE END_DATE
        
        try:
            # 步骤1：解析开始日期
            tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
            start_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
            
            # 步骤2：解析结束日期
            tmp_year, tmp_month, tmp_day = sys.argv[2].split("-")
            end_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
            
            # 步骤3：创建线程池并遍历日期范围
            # 使用with语句自动管理线程池生命周期
            with concurrent.futures.ThreadPoolExecutor() as executor:
                run_date = start_date
                
                # 每一天都检查一次
                while run_date <= end_date:
                    # 步骤4：检查是否为交易日（排除周末、假期等）
                    if trd.is_trade_date(run_date):
                        # 步骤5：提交任务到线程池
                        # executor.submit()：非阻塞提交，立即返回Future对象
                        executor.submit(run_fun, run_date, *args)
                        
                        # 步骤6：延迟2秒
                        # 目的：避免请求过快被API服务器限流
                        # 解释：每秒最多提交0.5个请求，不会造成过大压力
                        time.sleep(2)
                    
                    # 步骤7：下一天
                    run_date += datetime.timedelta(days=1)
                    
        except Exception as e:
            # 异常记录，但继续运行
            logging.error(f"run_template.run_with_args区间处理异常：{run_fun.__name__}{sys.argv}{e}")
            
    elif len(sys.argv) == 2:
        # ==================== 模式2：指定日期列表执行 ====================
        # python xxx.py 2024-01-01,2024-01-15,2024-01-30
        
        try:
            # 步骤1：按逗号分割日期列表
            dates = sys.argv[1].split(',')
            
            # 步骤2：创建线程池
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for date in dates:
                    try:
                        # 步骤3：解析每个日期字符串
                        tmp_year, tmp_month, tmp_day = date.split("-")
                        run_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
                        
                        # 步骤4：检查是否为交易日
                        if trd.is_trade_date(run_date):
                            # 步骤5：提交任务
                            executor.submit(run_fun, run_date, *args)
                            time.sleep(2)  # 延迟，避免限流
                            
                    except Exception as e:
                        logging.error(f"run_template.run_with_args日期解析异常：{date}{e}")
                        
        except Exception as e:
            logging.error(f"run_template.run_with_args列表处理异常：{run_fun.__name__}{sys.argv}{e}")
            
    else:
        # ==================== 模式3：当日执行 ====================
        # python xxx.py (无参数)
        # 自动获取最近交易日，根据函数名分发
        
        try:
            # 步骤1：获取最近的交易日期
            # run_date：最近的交易日（用于收盘后数据）
            # run_date_nph：盘后日期（用于实时数据）
            run_date, run_date_nph = trd.get_trade_date_last()
            
            # 步骤2：根据函数名前缀自动分发参数
            if run_fun.__name__.startswith('save_nph'):
                # save_nph 开头的函数：盘中/盘后任务
                # 第二个参数 False 表示非"盘前"
                # 这类任务用盘后日期，获取最新的实时数据
                run_fun(run_date_nph, False, *args)
                
            elif run_fun.__name__.startswith('save_after_close'):
                # save_after_close 开头的函数：收盘后才有的数据任务
                # 如：大宗交易、融资融券、龙虎榜等
                # 这类数据只有在收盘后才会更新
                run_fun(run_date, *args)
                
            else:
                # 其他函数：默认使用盘后日期
                run_fun(run_date_nph, *args)
                
        except Exception as e:
            logging.error(f"run_template.run_with_args当日处理异常：{run_fun.__name__}{sys.argv}{e}")


# ==================== 知识点总结 ====================
"""
核心知识点（任务调度和命令行参数）
===================================

1. 命令行参数处理
   - sys.argv：获取命令行参数列表
   - sys.argv[0]：脚本名
   - sys.argv[1:]：参数列表
   - len(sys.argv)：参数个数

2. 多种执行模式的设计
   - 灵活性：支持三种运行方式
   - 自动化：自动判断参数格式
   - 易用性：命令行简洁直观

3. 交易日的处理
   - is_trade_date()：判断是否交易日
   - 排除周末和假期
   - get_trade_date_last()：获取最近交易日

4. 并发管理
   - ThreadPoolExecutor：线程池
   - with语句：自动资源管理
   - executor.submit()：非阻塞提交
   - time.sleep()：请求间隔

5. 函数名前缀的自动分发
   - startswith()：检查前缀
   - 根据命名规范自动选择参数
   - 减少重复代码

常见问题Q&A
===========

Q1: 为什么需要2秒延迟？
A: - API服务器有请求频率限制
   - 避免被限流（HTTP 429错误）
   - 每秒0.5个请求是安全的速率
   - 可根据实际情况调整

Q2: 如何处理非交易日？
A: - trd.is_trade_date()自动过滤
   - 周末会跳过
   - 假期会跳过
   - 只处理交易日

Q3: 如何按指定日期执行？
A: python xxx.py 2024-01-01,2024-01-15
   # 指定多个日期，用逗号分隔

Q4: 如何批量补数据？
A: python xxx.py 2024-01-01 2024-01-31
   # 区间执行，会自动处理所有交易日

Q5: save_nph和save_after_close区别？
A: - save_nph：盘中/盘后实时数据
   - save_after_close：收盘后才有的数据
   - 参数不同（前者传run_date_nph）
   - 运行时机不同

Q6: 线程池的大小如何设定？
A: - 不指定时：默认CPU核数相关
   - 一般为 CPU核数 * 5
   - 可显式传入max_workers参数

Q7: 如何处理异常？
A: - 异常被捕获记录到日志
   - 继续执行下一个任务
   - 不会因为单个失败而中断全部

Q8: 为什么要用线程池而不是直接循环？
A: - 线程池可并发执行
   - 不需要等待每个任务完成
   - 大大加快总体处理速度

优化建议
=======
1. 可配置的延迟时间（参数化）
2. 可配置的线程池大小
3. 添加进度条显示
4. 添加预估完成时间
5. 支持暂停和恢复
6. 支持任务优先级
7. 支持断点重启
"""
