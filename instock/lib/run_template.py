#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# ==============================================
# 任务运行模板
# 为所有 job 模块提供统一的日期参数解析和任务调度能力
# 支持三种运行方式：区间批量、指定日期列表、当日自动
# ==============================================

import logging
import datetime
import concurrent.futures
import sys
import time
import instock.lib.trade_time as trd

__author__ = 'myh '
__date__ = '2023/3/10 '


"""
通用任务调度函数，解析命令行日期参数并执行指定函数

参数说明：
    run_fun (callable): 要执行的任务函数，签名为 run_fun(date, *args)
    *args: 传递给 run_fun 的额外参数

三种运行模式（根据命令行参数自动判断）：

模式1 - 日期区间批量执行（2个命令行参数）：
    python xxx.py 2023-03-01 2023-03-21
    遍历区间内每个交易日，用线程池并发执行，每个任务间隔2秒

模式2 - 指定日期列表（1个命令行参数，逗号分隔）：
    python xxx.py 2023-03-01,2023-03-02,2023-03-03
    只执行指定的交易日

模式3 - 当日执行（无命令行参数）：
    python xxx.py
    自动获取最近交易日，根据函数名前缀分发：
    - save_nph_xxx  → 传入 run_date_nph（盘后日期），before=False
    - save_after_close_xxx → 传入 run_date（收盘日期）
    - 其他 → 传入 run_date_nph
"""
# 通用函数，获得日期参数，支持批量作业。
def run_with_args(run_fun, *args):
    if len(sys.argv) == 3:
        # 区间作业 python xxx.py 2023-03-01 2023-03-21
        tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
        start_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
        tmp_year, tmp_month, tmp_day = sys.argv[2].split("-")
        end_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
        run_date = start_date
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                while run_date <= end_date:
                    if trd.is_trade_date(run_date):
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)  # 间隔2秒，避免请求过快被限流
                    run_date += datetime.timedelta(days=1)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
    elif len(sys.argv) == 2:
        # N个时间作业 python xxx.py 2023-03-01,2023-03-02
        dates = sys.argv[1].split(',')
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for date in dates:
                    tmp_year, tmp_month, tmp_day = date.split("-")
                    run_date = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day)).date()
                    if trd.is_trade_date(run_date):
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
    else:
        # 当前时间作业 python xxx.py
        try:
            # run_date: 最近交易日（收盘后数据用这个）
            # run_date_nph: 盘后日期（实时数据用这个）
            run_date, run_date_nph = trd.get_trade_date_last()
            if run_fun.__name__.startswith('save_nph'):
                # save_nph 开头：盘中/盘后任务，第二参数 False 表示非"盘前"
                run_fun(run_date_nph, False)
            elif run_fun.__name__.startswith('save_after_close'):
                # save_after_close 开头：收盘后才有的数据（如大宗交易）
                run_fun(run_date, *args)
            else:
                run_fun(run_date_nph, *args)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
