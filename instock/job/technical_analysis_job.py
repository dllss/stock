#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
技术分析脚本 - 仅运行技术分析和选股任务(不包含基础数据抓取)
适用场景: 已有基础数据,只需要计算指标和选股
预计耗时: 30-50分钟

前置条件:
  - 已运行 execute_daily_job.py(基础数据)
  - 已运行 historical_data_job.py(历史K线数据)

使用方法:
  python instock/job/technical_analysis_job.py
  
说明:
  本脚本等价于执行 execute_daily_job.py 中的以下任务:
  - 任务2: 计算技术指标 (indicators_data_daily_job)
  - 任务3: 识别K线形态 (klinepattern_data_daily_job)
  - 任务4: 策略选股 (strategy_data_daily_job)
"""

import sys
import os
import time
from datetime import datetime

# 添加项目根目录到Python路径
# instock/job/technical_analysis_job.py -> instock/job -> instock -> project_root
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

def run_indicators():
    """步骤1: 计算技术指标"""
    print_step(1, 3, "计算技术指标")
    
    try:
        # 保存原始argv并清空,避免子模块解析我们的参数
        original_argv = sys.argv.copy()
        sys.argv = [sys.argv[0]]  # 只保留脚本名
        
        from instock.job import indicators_data_daily_job
        indicators_data_daily_job.main()
        
        # 恢复原始argv
        sys.argv = original_argv
        
        print("[OK] 技术指标计算完成")
        return True
    except Exception as e:
        print(f"\n[WARN] 技术指标计算失败: {e}")
        print("请检查日志文件: instock/log/stock_execute_job.log")
        
        # 询问是否继续
        choice = input("\n是否继续执行后续任务? (y/n): ").strip().lower()
        if choice != 'y':
            return False
        return True

def run_kline_pattern():
    """步骤2: K线形态识别"""
    print_step(2, 3, "识别K线形态")
    
    try:
        original_argv = sys.argv.copy()
        sys.argv = [sys.argv[0]]
        
        from instock.job import klinepattern_data_daily_job
        klinepattern_data_daily_job.main()
        
        sys.argv = original_argv
        
        print("[OK] K线形态识别完成")
        return True
    except Exception as e:
        print(f"\n[WARN] K线形态识别失败: {e}")
        print("请检查日志文件: instock/log/stock_execute_job.log")
        
        # 询问是否继续
        choice = input("\n是否继续执行后续任务? (y/n): ").strip().lower()
        if choice != 'y':
            return False
        return True

def run_strategy():
    """步骤3: 策略选股"""
    print_step(3, 3, "策略选股")
    
    try:
        original_argv = sys.argv.copy()
        sys.argv = [sys.argv[0]]
        
        from instock.job import strategy_data_daily_job
        strategy_data_daily_job.main()
        
        sys.argv = original_argv
        
        print("[OK] 策略选股完成")
        return True
    except Exception as e:
        print(f"\n[ERROR] 策略选股失败: {e}")
        print("请检查日志文件: instock/log/stock_execute_job.log")
        return False

def main():
    """主函数"""
    print_separator("开始技术分析(不含基础数据)")
    print("\n说明: 本脚本执行 execute_daily_job.py 中的任务2-4:")
    print("  - 任务2: 计算技术指标")
    print("  - 任务3: 识别K线形态")
    print("  - 任务4: 策略选股")
    print("\n注意: 此脚本假设基础数据和历史K线已存在")
    print("如果尚未获取基础数据,请先运行 quick_update.py\n")
    
    # 检查是否有 --no-wait 参数
    if '--no-wait' not in sys.argv:
        input("按回车键继续...")
    else:
        print("跳过等待,直接开始执行...\n")
    
    # 记录开始时间
    start_time = datetime.now()
    print(f"\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行三个步骤
    success = True
    
    # 步骤1: 技术指标
    if not run_indicators():
        success = False
    
    # 步骤2: K线形态
    if success and not run_kline_pattern():
        success = False
    
    # 步骤3: 策略选股
    if success and not run_strategy():
        success = False
    
    # 记录结束时间
    end_time = datetime.now()
    duration = end_time - start_time
    
    print_separator("技术分析完成!")
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
