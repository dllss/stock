#!/usr/local/bin/python
# -*- coding: utf-8 -*-


import logging
import datetime
import concurrent.futures
import sys
import time
import instock.lib.trade_time as trd

__author__ = 'myh '
__date__ = '2023/3/10 '


# 通用函数，获得日期参数，支持批量作业。
# 全局标志：控制日期日志只打印一次
_date_logged = False

def run_with_args(run_fun, *args):
    global _date_logged
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
                        # 直接传递额外的参数（如 before=False）
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
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
                    # 只在首次调用时打印日期日志
                    if not _date_logged:
                        logging.info(f"📅 解析到日期: {run_date}")
                        _date_logged = True
                    if trd.is_trade_date(run_date):
                        logging.info(f"{run_date} 是交易日，开始执行...")
                        # 直接传递额外的参数（如 before=False）
                        executor.submit(run_fun, run_date, *args)
                        time.sleep(2)
                    else:
                        logging.warning(f"{run_date} 不是交易日，跳过")
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
            import traceback
            logging.error(traceback.format_exc())
    else:
        # 当前时间作业 python xxx.py
        try:
            run_date, run_date_nph = trd.get_trade_date_last()
            # 直接传递额外的参数（如 before=False）
            run_fun(run_date_nph, *args)
        except Exception as e:
            logging.error(f"run_template.run_with_args处理异常：{run_fun}{sys.argv}{e}")
