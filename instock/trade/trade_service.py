#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动交易服务（第八层 - 交易执行层）
==================================

模块功能：
---------
启动自动交易引擎，连接券商API，执行策略生成的交易信号。
这是整个自动化交易系统的运行入口。

核心职责：
1. 初始化交易引擎
2. 加载交易策略
3. 连接券商API
4. 监听交易信号
5. 执行买卖操作
6. 记录交易日志

架构设计：
---------

MainEngine（主引擎）
├── broker（券商连接）
│   ├── 国金证券（gf_client）
│   └── 其他券商API
├── strategy（策略管理）
│   ├── 加载策略模块
│   ├── 动态重载
│   └── 执行策略
├── order（订单管理）
│   ├── 生成订单
│   ├── 发送订单
│   └── 撤销订单
└── position（持仓管理）
    ├── 跟踪持仓
    ├── 计算收益
    └── 风险控制

运行流程：
--------

启动 trade_service.py
    ↓
读取配置文件（trade_client.json）
    ↓
初始化日志系统
    ↓
创建MainEngine实例
    ↓
连接券商API（国金证券）
    ↓
加载交易策略模块
    ↓
设置文件监视（自动重载）
    ↓
启动交易引擎
    ↓
等待交易信号并执行
    ↓
记录交易结果
    ↓
持续运行直到手动停止

配置文件（trade_client.json）：
------------------------------

包含以下关键配置：
- 账户信息（账号、密码）
- 券商API参数
- 交易品种设定
- 风险控制参数
- 持仓限制

格式示例：
{
  "broker": "gf_client",
  "account": {
    "username": "your_username",
    "password": "your_password",
    "portfolio_code": "your_portfolio"
  },
  "trading": {
    "symbols": ["000001", "000002"],  # 交易品种
    "max_positions": 10,  # 最大持仓数
    "max_position_size": 100000,  # 单个持仓最大金额
    "stop_loss": -0.05  # 止损线（-5%）
  }
}

日志系统：
--------
- 日志文件：instock/log/stock_trade.log
- 日志级别：DEBUG、INFO、WARNING、ERROR
- 记录内容：
  - 策略执行
  - 交易信号
  - 订单状态
  - 持仓变化
  - 风险事件
  - 错误信息

策略加载机制：
-----------

默认加载位置：instock/trade/strategies/
支持的策略文件：*.py

动态重载（开发模式）：
- is_watch_strategy = True
- 检测策略文件修改
- 自动重新加载
- 无需重启服务
- 加快开发效率

手动执行流程：
-----------

# 启动交易服务
python trade_service.py

# 或通过脚本调用
from instock.trade.trade_service import main
main()

常见使用场景：
-----------

场景1：生产环境自动交易
python trade_service.py
配置：is_watch_strategy = False（不开启文件监视）

场景2：开发测试
python trade_service.py
配置：is_watch_strategy = True（开启文件监视，快速调试）

场景3：定时启动（cron任务）
* 9 * * * python /path/to/trade_service.py
（每天9点启动交易）

场景4：长期运行（后台进程）
nohup python trade_service.py > trade.log 2>&1 &

风险管理：
--------

1. 止损保护
   - 设定止损线（如-5%）
   - 自动平仓保护账户

2. 持仓限制
   - 单个股票最大持仓
   - 总持仓数限制
   - 防止过度集中

3. 账户保护
   - 最大亏损限制
   - 资金上限控制
   - 自动暂停机制

4. 监控告警
   - 异常交易告警
   - 持仓风险告警
   - API连接告警

国金证券API（gf_client）：
------------------------

支持功能：
- 实时行情查询
- 历史数据获取
- 委托下单
- 委托撤单
- 成交查询
- 持仓查询
- 资金查询
- 风险等级查询

连接参数：
- 服务器地址
- 登录账号
- 登录密码
- 通讯加密

数据安全：
--------
- 配置文件不上传版本库
- 账户信息加密存储
- 日志脱敏处理
- API请求加密传输
- 本地存储密码加密

故障恢复：
--------

异常断连：
- 自动重连机制
- 连接重试（最多3次）
- 指数退避延迟

数据同步：
- 启动时同步账户状态
- 定时更新持仓信息
- 交易记录核对

性能指标：
--------
- 策略执行延迟：<100ms
- 订单发送延迟：<200ms
- 数据更新频率：实时
- 最大并发订单数：50

优化建议：
--------
1. 添加订单预审核
2. 支持多策略并发
3. 实现止盈止损
4. 添加资金使用率限制
5. 实现策略评分
6. 支持组合交易
"""

# ==================== 导入系统库 ====================
import os.path  # 路径操作
import sys  # 系统操作

# ==================== 路径配置 ====================
# 步骤1：获取当前文件的上级目录（instock目录）
cpath_current = os.path.dirname(os.path.dirname(__file__))

# 步骤2：获取上级目录的上级目录（项目根目录）
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))

# 步骤3：将项目路径添加到Python搜索路径
# 这样可以在任何位置运行脚本都能正确导入instock模块
sys.path.append(cpath)

# 步骤4：配置文件路径
need_data = os.path.join(cpath_current, 'config', 'trade_client.json')

# 步骤5：日志文件路径
log_filepath = os.path.join(cpath_current, 'log', 'stock_trade.log')

# ==================== 导入项目模块 ====================
from instock.trade.robot.engine.main_engine import MainEngine  # 交易引擎主类
from instock.trade.robot.infrastructure.default_handler import DefaultLogHandler  # 日志处理器

__author__ = 'myh '
__date__ = '2023/4/10 '


# ==================== 交易服务主函数 ====================

def main():
    """
    启动自动交易服务
    
    执行步骤：
    -------
    1. 选择券商（国金证券）
    2. 初始化日志系统
    3. 创建交易引擎
    4. 加载交易策略
    5. 启动交易服务
    
    配置说明：
    --------
    - broker：券商标识（'gf_client'表示国金证券）
    - need_data：交易配置文件路径（包含账户信息）
    - log_filepath：日志文件保存路径
    - is_watch_strategy：是否监视策略文件变化
      * True：开发模式，文件改动自动重载
      * False：生产模式，需要重启才能生效
    
    运行方式：
    --------
    直接运行：python trade_service.py
    或调用：main()
    
    异常处理：
    --------
    如果启动失败，检查：
    1. trade_client.json是否存在
    2. 账户信息是否正确
    3. 网络连接是否正常
    4. 券商API是否可用
    5. 检查日志文件获取详细错误信息
    """
    
    # 步骤1：指定使用的券商
    broker = 'gf_client'  # 国金证券
    
    # 步骤2：创建日志处理器
    # 参数说明：
    #   - name：日志模块名称
    #   - log_type：日志类型（'file'表示文件日志，'console'表示控制台）
    #   - filepath：日志文件路径
    log_handler = DefaultLogHandler(
        name='交易服务',
        log_type='file',
        filepath=log_filepath
    )
    
    # 步骤3：创建主交易引擎实例
    # 参数：
    #   - broker：券商标识
    #   - need_data：配置文件路径
    #   - log_handler：日志处理器
    m = MainEngine(broker, need_data, log_handler)
    
    # 步骤4：设置策略文件监视
    # 注意：仅用于开发测试，生产环境应设置为False
    m.is_watch_strategy = True
    
    # 步骤5：加载交易策略
    # 从 instock/trade/strategies/ 目录加载所有策略文件
    m.load_strategy()
    
    # 步骤6：启动交易引擎
    # 引擎将持续运行，直到手动停止（Ctrl+C）
    m.start()


# ==================== 程序入口 ====================

if __name__ == '__main__':
    """
    当此文件作为主程序运行时执行main()函数
    
    运行方式：
    --------
    python trade_service.py
    
    停止方式：
    --------
    按 Ctrl+C 停止程序
    """
    main()
