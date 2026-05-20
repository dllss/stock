#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化交易系统启动入口 - 交易服务主程序
==========================================

功能说明：
本模块是自动化交易系统的启动入口，负责：
1. 初始化交易环境
2. 加载配置文件
3. 创建日志处理器
4. 启动交易引擎
5. 加载并运行交易策略

系统架构：
┌─────────────────────────────────────┐
│     trade_service.py (本文件)        │
│     - 系统启动入口                    │
│     - 环境初始化                      │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│      MainEngine (主引擎)             │
│     - 策略管理                       │
│     - 事件驱动                       │
│     - 订单执行                       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│    Strategies (交易策略)             │
│     - 买入策略                       │
│     - 卖出策略                       │
│     - 风控策略                       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│    Broker Client (券商客户端)        │
│     - gf_client (广发证券)           │
│     - 其他券商客户端                  │
└─────────────────────────────────────┘

核心组件：
1. MainEngine：交易主引擎
   - 策略加载和管理
   - 事件循环和调度
   - 订单发送和执行
   - 账户资金管理

2. DefaultLogHandler：日志处理器
   - 文件日志记录
   - 控制台输出
   - 日志级别控制

3. Strategy：交易策略
   - 买入信号识别
   - 卖出信号识别
   - 仓位管理
   - 风险控制

配置说明：
trade_client.json 配置文件包含：
- 券商账户信息
- 交易参数设置
- 策略配置
- 风控参数

使用场景：
- 自动化股票交易
- 量化策略执行
- 程序化交易
- 算法交易实现

运行方式：
```bash
# 直接运行
python trade_service.py

# 后台运行（Linux）
nohup python trade_service.py > trade.log 2>&1 &

# 使用supervisor管理
supervisorctl start stock_trade
```

注意事项：
1. 需要先配置 trade_client.json 文件
2. 确保券商客户端可用
3. 建议先在模拟环境测试
4. 生产环境关闭 is_watch_strategy
5. 注意资金安全和风险控制

依赖关系：
- instock.trade.robot.engine.main_engine：主引擎
- instock.trade.robot.infrastructure.default_handler：日志处理器
- config/trade_client.json：交易配置
"""

import os.path
import sys

# ==================== 路径配置 ====================
# 在项目运行时，临时将项目路径添加到环境变量
# 这样可以正确导入项目中的模块

# 获取当前文件所在目录的父目录（instock目录）
cpath_current = os.path.dirname(os.path.dirname(__file__))

# 获取项目的根目录
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))

# 将项目根目录添加到Python路径
# 这样可以从任何地方导入 instock 包中的模块
sys.path.append(cpath)

# ==================== 配置文件路径 ====================
# 交易客户端配置文件路径
# 包含券商账户、交易参数、策略配置等
need_data = os.path.join(cpath_current, 'config', 'trade_client.json')

# 日志文件路径
# 记录交易日志、错误信息、策略执行情况等
log_filepath = os.path.join(cpath_current, 'log', 'stock_trade.log')

# ==================== 导入核心模块 ====================
from instock.trade.robot.engine.main_engine import MainEngine
from instock.trade.robot.infrastructure.default_handler import DefaultLogHandler

__author__ = 'myh '
__date__ = '2023/4/10 '


# ==================== 主函数 ====================

def main():
    """
    交易系统主函数
    
    执行流程：
    1. 设置券商类型（广发证券）
    2. 创建日志处理器
    3. 初始化主引擎
    4. 启用策略热重载（开发环境）
    5. 加载交易策略
    6. 启动交易引擎
    
    注意：
    - is_watch_strategy=True 会在策略文件改动时自动重载
    - 生产环境建议设置为 False，避免性能损耗
    """
    
    # 券商类型：广发证券
    # 支持多种券商，需要对应的客户端实现
    broker = 'gf_client'
    
    # 创建日志处理器
    # name='交易服务'：日志名称
    # log_type='file'：日志类型为文件日志
    # filepath=log_filepath：日志文件路径
    log_handler = DefaultLogHandler(
        name='交易服务', 
        log_type='file', 
        filepath=log_filepath
    )
    
    # 初始化主引擎
    # broker：券商类型
    # need_data：配置文件路径
    # log_handler：日志处理器
    m = MainEngine(broker, need_data, log_handler)
    
    # 启用策略热重载
    # True：策略文件改动时自动重新加载
    # 优点：开发调试方便，无需重启服务
    # 缺点：生产环境可能影响稳定性，有性能开销
    # 建议：开发环境True，生产环境False
    m.is_watch_strategy = True
    
    # 加载交易策略
    # 从配置文件中读取策略列表
    # 实例化策略对象
    # 注册到主引擎
    m.load_strategy()
    
    # 启动交易引擎
    # 初始化券商连接
    # 启动事件循环
    # 开始监听市场数据
    # 执行交易策略
    m.start()


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    main()
