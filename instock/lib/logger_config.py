# -*- coding: utf-8 -*-
"""
统一日志配置：所有 job 同时输出到终端和文件
"""
import logging
import os
import sys


"""
配置日志：同时输出到终端和 instock/log/ 下文件。
单独运行任意 job 时在 __main__ 里调用即可。
返回日志文件完整路径。
"""
def setup_job_logging(log_filename="stock_execute_job.log"):
    _instock_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _log_dir = os.path.join(_instock_dir, "log")
    if not os.path.exists(_log_dir):
        os.makedirs(_log_dir)
    _log_file = os.path.join(_log_dir, log_filename)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(_log_file, encoding="utf-8"),
        ],
        force=True,
    )
    return _log_file
