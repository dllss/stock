#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速更新脚本 - 获取最新的基础数据
适用场景: 交易日盘中或盘后快速获取最新数据
预计耗时: 10-20分钟

使用方法:
  python instock/job/quick_update_job.py
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
script_dir = os.path.dirname(os.path.abspath(__file__))  # instock/job
instock_dir = os.path.dirname(script_dir)  # instock
project_root = os.path.dirname(instock_dir)  # project root

# 确保instock目录在Python路径中
if instock_dir not in sys.path:
    sys.path.insert(0, instock_dir)

# 也添加项目根目录
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def print_separator(title=""):
    """打印分隔线"""
    print("\n" + "=" * 60)
    if title:
        print(f"   {title}")
        print("=" * 60)

def print_step(step_num, total_steps, description):
    """打印步骤信息"""
    print(f"\n[{step_num}/{total_steps}] {description}...")
    print("-" * 60)

def run_basic_data():
    """步骤1: 基础数据"""
    print_step(1, 2, "获取基础数据")
    
    try:
        from instock.job import basic_data_daily_job
        basic_data_daily_job.main()
        print("[OK] 基础数据获取完成")
        return True
    except Exception as e:
        print(f"\n[ERROR] 基础数据获取失败: {e}")
        print("请检查日志文件: instock/log/stock_execute_job.log")
        return False

def run_other_data():
    """步骤2: 其他数据"""
    print_step(2, 2, "获取其他数据")
    
    try:
        from instock.job import basic_data_other_daily_job
        basic_data_other_daily_job.main()
        print("[OK] 其他数据获取完成")
        return True
    except Exception as e:
        print(f"\n[WARN] 其他数据获取失败: {e}")
        print("请检查日志文件: instock/log/stock_execute_job.log")
        return False

def main():
    """主函数"""
    print_separator("开始快速更新")
    print("\n此脚本将获取最新的基础数据\n")
    
    # 检查是否有 --no-wait 参数
    if '--no-wait' not in sys.argv:
        input("按回车键继续...")
    else:
        print("跳过等待,直接开始执行...\n")
    
    # 记录开始时间
    start_time = datetime.now()
    print(f"\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行步骤
    success = True
    
    # 步骤1: 基础数据
    if not run_basic_data():
        success = False
    
    # 步骤2: 其他数据
    if success and not run_other_data():
        success = False
    
    # 记录结束时间
    end_time = datetime.now()
    duration = end_time - start_time
    
    print_separator("快速更新完成!")
    print(f"\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {duration}")
    print(f"\n详细日志: instock/log/stock_execute_job.log")
    
    if success:
        print("\n[OK] 所有任务执行成功!")
    else:
        print("\n[WARN] 部分任务执行失败,请检查日志")
    
    print()

if __name__ == "__main__":
    main()
