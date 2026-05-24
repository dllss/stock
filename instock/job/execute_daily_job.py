#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日任务总调度模块 - 数据处理流程编排
======================================

功能说明：
本模块是整个系统的数据处理调度中心，负责协调和编排所有数据处理任务。
按照依赖关系和执行顺序，自动化完成从数据采集到分析的全流程。

核心职责：
1. 任务调度：按顺序执行多个数据处理任务
2. 并行优化：使用多线程加速独立任务
3. 日志记录：跟踪每个任务的执行情况
4. 错误处理：捕获异常，保证流程继续
5. 时间统计：记录各阶段耗时

系统架构 - 完整数据处理流程：

┌─────────────────────────────────────────────────┐
│  第1步：init_job                                 │
│  初始化数据库和表结构                             │
│  - 创建必要的数据库                              │
│  - 建表（如果不存在）                            │
│  - 创建索引                                      │
└────────────────┬────────────────────────────────┘
                 ↓ (串行)
┌─────────────────────────────────────────────────┐
│  第2.1步：股票和ETF基础数据                         │
│  抓取基础行情数据                                   │
│  - 股票基础数据（cn_stock_spot）                    │
│  - ETF基础数据（cn_etf_spot）                      │
└────────────────┬────────────────────────────────┘
                 ↓ (串行)
┌─────────────────────────────────────────────────┐
│  第2.2步：selection_data_daily_job               │
│  抓取综合选股数据                                 │
│  - 东方财富选股数据                              │
│  - 题材概念分类                                  │
└────────────────┬────────────────────────────────┘
                 ↓ (串行)
┌─────────────────────────────────────────────────┐
│  第3步：并行执行（使用多线程）                     │
│  ├─ basic_data_other_daily_job   (其他基础数据)   │
│  │  - cn_stock_lhb: 龙虎榜明细                    │
│  │  - cn_stock_top: 龙虎榜汇总                    │
│  │  - cn_stock_fund_flow: 个股资金流向            │
│  │  - cn_stock_fund_flow_industry: 行业资金流向   │
│  │  - cn_stock_fund_flow_concept: 概念资金流向    │
│  │  - cn_stock_bonus: 分红配送                    │
│  │  - cn_stock_chip_race_open: 早盘抢筹           │
│  │  - cn_stock_limitup_reason: 涨停原因           │
│  ├─ indicators_data_daily_job    (计算技术指标)   │
│  │  - 75种技术指标                                │
│  │  - MACD/KDJ/BOLL等                            │
│  ├─ klinepattern_data_daily_job  (识别K线形态)   │
│  │  - 61种K线形态                                │
│  │  - 早晨之星/黄昏之星等                         │
│  └─ strategy_data_daily_job      (策略选股)      │
│     - 多因子选股策略                              │
│     - 技术面选股                                  │
└────────────────┬────────────────────────────────┘
                 ↓ (串行)
┌─────────────────────────────────────────────────┐
│  第4步：backtest_data_daily_job                  │
│  回测验证                                         │
│  - 计算策略收益率                                │
│  - 评估策略效果                                  │
└────────────────┬────────────────────────────────┘
                 ↓ (串行)
┌─────────────────────────────────────────────────┐
│  第5步：basic_data_after_close_daily_job         │
│  收盘后数据                                       │
│  - cn_stock_blocktrade: 大宗交易                  │
│  - cn_stock_chip_race_end: 尾盘抢筹              │
└─────────────────────────────────────────────────┘

设计原理：

1. 串行任务（有依赖关系）：
   ```
   init → basic_data → selection → 并行任务 → backtest → after_close
   ```
   - 必须先初始化数据库，才能写入数据
   - 必须先有基础数据，才能计算指标
   - 必须先有策略结果，才能回测验证
   - 某些数据只能在收盘后获取

2. 并行任务（相互独立）：
   ```
   Thread 1: other_data (其他基础数据)
   Thread 2: indicators (技术指标)
   Thread 3: kline_pattern (K线形态)
   Thread 4: strategy (策略选股)
   ```
   - 这4个任务互不依赖，可以同时进行
   - 利用多核CPU，大幅提升效率
   - 单线程：30分钟 → 多线程：5-10分钟

3. 性能优化策略：
   - ThreadPoolExecutor：Python标准库的多线程池
   - max_workers：控制并发线程数
   - submit()：提交任务到线程池
   - as_completed()：按完成顺序获取结果

运行方式：

方式1：处理最新交易日数据（默认）
```bash
python execute_daily_job.py
```

方式2：处理指定日期
```bash
python execute_daily_job.py 2024-01-01
```

方式3：处理多个日期（逗号分隔）
```bash
python execute_daily_job.py 2024-01-01,2024-01-02,2024-01-03
```

方式4：处理日期区间
```bash
python execute_daily_job.py 2024-01-01 2024-01-10
```

方式5：定时任务（crontab）
```bash
# 每天下午6点执行（收盘后）
0 18 * * * cd /path/to/stock && python instock/job/execute_daily_job.py >> /var/log/stock_job.log 2>&1
```

参数说明：
- 无参数：处理今天
- 单个日期：YYYY-MM-DD格式
- 多个日期：用逗号分隔
- 日期区间：起始日期 结束日期

日志系统：

日志文件：stock_execute_job.log
日志位置：instock/log/目录

日志内容：
```
[2024-01-01 18:00:00] INFO - ========== 开始执行每日任务 ==========
[2024-01-01 18:00:00] INFO - 任务日期：2024-01-01
[2024-01-01 18:00:01] INFO - [1/7] 开始执行：初始化任务
[2024-01-01 18:00:05] INFO - [1/7] 完成：初始化任务 (耗时: 4秒)
[2024-01-01 18:00:05] INFO - [2/7] 开始执行：基础数据任务
[2024-01-01 18:02:30] INFO - [2/7] 完成：基础数据任务 (耗时: 145秒)
...
[2024-01-01 18:15:00] INFO - ========== 所有任务执行完成 ==========
[2024-01-01 18:15:00] INFO - 总耗时：900秒 (15分钟)
```

关键特性：
1. 详细的时间统计（每个任务耗时）
2. 清晰的步骤标识（[1/7], [2/7]等）
3. 异常捕获和记录
4. 支持断点续传（某个任务失败不影响其他）

使用场景：

1. 日常数据更新：
   - 每天收盘后自动执行
   - 保持数据最新
   - 为第二天的分析做准备

2. 历史数据补全：
   - 批量处理历史日期
   - 填补缺失数据
   - 重新计算指标

3. 系统初始化：
   - 新部署时建立数据结构
   - 导入历史数据
   - 验证系统完整性

4. 数据修复：
   - 某天的数据有问题
   - 重新执行该日期的任务
   - 覆盖错误数据

注意事项：

1. 执行时间：
   - 建议在收盘后执行（15:30之后）
   - 避免在交易时段占用资源
   - 考虑API限流问题

2. 资源消耗：
   - CPU：并行任务时较高
   - 内存：约500MB-1GB
   - 网络：大量API请求
   - 磁盘：数据存储需求

3. 依赖条件：
   - MySQL数据库正常运行
   - 网络连接稳定
   - 足够的磁盘空间
   - Python环境配置正确

4. 常见问题：
   - API限流：增加延迟或减少并发
   - 数据库连接失败：检查MySQL服务
   - 内存不足：减少并行任务数
   - 网络超时：增加重试机制

5. 监控建议：
   - 定期检查日志文件大小
   - 监控数据库存储空间
   - 设置任务失败告警
   - 记录执行成功率

性能调优：

1. 调整并发数：
```python
# 修改 max_workers 参数
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    # 4个线程并行
```

2. 优化数据库：
```sql
-- 添加索引加速查询
CREATE INDEX idx_code_date ON stock_hist(code, date);
-- 调整缓冲池大小
SET GLOBAL innodb_buffer_pool_size = 2G;
```

3. 分批处理：
```python
# 如果数据量太大，可以分批
for batch in batches:
    process_batch(batch)
```

4. 缓存优化：
```python
# 使用Redis缓存常用数据
# 减少重复的API调用
```

依赖关系：
- instock.job.init_job：数据库初始化
- instock.job.data_tasks.cn_stock_spot_job：股票基础数据（cn_stock_spot）
- instock.job.data_tasks.cn_etf_spot_job：ETF基础数据（cn_etf_spot）
- instock.job.data_tasks.cn_stock_selection_job：综合选股数据（cn_stock_selection）
- instock.job.basic_data_other_daily_job：其他基础数据
- instock.job.indicators_data_daily_job：指标计算
- instock.job.klinepattern_data_daily_job：形态识别
- instock.job.strategy_data_daily_job：策略选股
- instock.job.backtest_data_daily_job：回测验证
- instock.job.basic_data_after_close_daily_job：收盘数据

最佳实践：
1. 定期执行：建立自动化调度（crontab/systemd timer）
2. 监控日志：设置日志轮转和告警
3. 备份数据：定期备份数据库
4. 测试验证：先在测试环境验证
5. 版本管理：记录每次代码变更
6. 文档更新：同步更新相关文档
"""

# ==================== 导入必需的库 ====================
import time  # time：时间测量，用于统计任务耗时
import datetime  # datetime：日期时间处理，解析和管理日期参数
import concurrent.futures  # concurrent.futures：并发执行（多线程），提高并行任务效率
import logging  # logging：日志记录，跟踪任务执行情况
import os.path  # os.path：文件路径处理，构建日志文件路径
import sys  # sys：系统相关，处理命令行参数

# ==================== 路径和日志配置 ====================
# 在项目运行时，临时将项目路径添加到环境变量
cpath_current = os.path.dirname(os.path.dirname(__file__))  # 当前目录的上级目录
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))  # 项目根目录
sys.path.append(cpath)  # 添加到Python搜索路径

# 配置日志：同时输出到终端和文件
from instock.lib.logger_config import setup_job_logging
log_path = os.path.dirname(setup_job_logging())  # 日志目录，供末尾 print 使用

# ==================== 导入各个任务模块 ====================
# 这些模块分别负责不同的数据处理任务
from instock.job import init_job  # 初始化任务
from instock.job import basic_data_other_daily_job  # 其他基础数据任务
from instock.job import basic_data_after_close_daily_job  # 收盘后数据任务
from instock.job import adjustment_data_daily_job  # 除权股票前复权K线修复任务
from instock.job import indicators_data_daily_job  # 指标计算任务
from instock.job import strategy_data_daily_job  # 策略选股任务
from instock.job import backtest_data_daily_job  # 回测任务
from instock.job import klinepattern_data_daily_job  # K线形态识别任务
from instock.job import data_validation_daily_job  # 每日数据表验证任务

# 独立任务模块（data_tasks文件夹）
from instock.job.data_tasks import cn_stock_spot_job  # 股票基础数据任务 - cn_stock_spot
from instock.job.data_tasks import cn_etf_spot_job  # ETF基础数据任务 - cn_etf_spot
from instock.job.data_tasks import cn_stock_selection_job  # 综合选股数据任务 - cn_stock_selection
import instock.lib.run_template as runt  # 日期参数运行模板
import instock.lib.trade_time as trd  # 交易日工具

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 主函数 ====================

def _to_date(value):
    """
    将 run_template 传入的日期转换为 date，供每日调度计算除权查询窗口。
    """
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date value: {value!r}")


def repair_adjustment_kline_for_daily_date(run_date):
    """
    每日总调度的除权修复包装函数。

    单独执行 adjustment_data_daily_job 时默认查近 30 天；
    每日总调度只查“上一个交易日 ~ 当前运行日”，并把明确的查询窗口传给修复任务。
    """
    ex_dividend_end_date = _to_date(run_date)
    ex_dividend_start_date = trd.get_previous_trade_date(ex_dividend_end_date)
    adjustment_data_daily_job.repair_ex_dividend_kline_data(
        ex_dividend_end_date,
        ex_dividend_start_date=ex_dividend_start_date,
        ex_dividend_end_date=ex_dividend_end_date,
    )


def run_adjustment_kline_repair_for_daily_job():
    """
    使用 execute_daily_job 的日期参数运行除权 K 线修复。
    """
    runt.run_with_args(repair_adjustment_kline_for_daily_date)

"""
任务调度主函数
功能说明：
按照预定顺序执行所有数据处理任务
串行任务按顺序执行，并行任务使用线程池同时执行
执行步骤：
1. 初始化数据库
2. 抓取基础数据
3. 抓取综合选股数据
4. 并行执行：其他基础数据、指标计算、形态识别、策略选股
5. 回测验证
6. 收盘后数据
性能优化：
- 使用并发执行加速处理
- 记录执行时间，便于优化
日志记录：
- 记录任务开始时间
- 记录任务完成时间和耗时
- 各子任务也有详细日志
使用示例：
# 方式1：直接运行脚本（使用当前交易日）
python execute_daily_job.py
# 方式2：指定日期
python execute_daily_job.py 2024-01-01
# 方式3：多个日期
python execute_daily_job.py 2024-01-01,2024-01-02,2024-01-03
# 方式4：日期区间
python execute_daily_job.py 2024-01-01 2024-01-10
"""
def main():
    # 导入任务工具
    from instock.job.task_utils import log_task_start
    
    # 步骤0: 记录任务开始时间
    start = time.time()  # 记录开始时间戳（秒）
    _start = datetime.datetime.now()  # 获取当前日期时间
    
    # 任务开始日志 - 总调度任务
    log_task_start("daily_job_scheduler", "每日数据抓取与分析总调度任务")
    
    # 记录任务开始日志
    # strftime()：格式化日期时间为字符串
    # %Y-%m-%d：年-月-日
    # %H:%M:%S：时-分-秒
    # %f：微秒
    logging.info("######## 任务执行时间: %s #######" % _start.strftime("%Y-%m-%d %H:%M:%S.%f"))
    # ==================== 步骤1：初始化数据库 ====================
    log_task_start("database_init", "初始化数据库和表结构")
    # 创建数据库和所有表结构
    # 如果数据库已存在，则跳过
    # 如果表已存在，则跳过
    init_job.main()
    
    # ==================== 步骤2.1：抓取基础数据 ====================
    log_task_start("basic_data_fetch", "抓取股票和ETF基础数据")
    # 基础数据是后续所有任务的基础
    # 包括：
    # - 每日股票数据（价格、成交量、市值等）- cn_stock_spot
    cn_stock_spot_job.main()  # 股票基础数据
    # - 每日ETF数据 - cn_etf_spot
    cn_etf_spot_job.main()    # ETF基础数据
    
    # ==================== 步骤2.2：抓取综合选股数据 ====================
    log_task_start("selection_data_fetch", "抓取综合选股数据（200+指标）")
    # 综合选股数据（200+个选股指标）- cn_stock_selection
    cn_stock_selection_job.main()
    
    # ==================== 步骤3：串行执行多个任务 ====================
    log_task_start("sequential_tasks", "串行执行其他基础数据抓取任务")
    # 这些任务相互独立，串行执行以保证日志顺序清晰、便于调试
    
    # 任务1：其他基础数据（8个数据表）
    # 包括：龙虎榜明细/汇总、个股/行业/概念资金流向、分红配送、早盘抢筹、涨停原因
    log_task_start("other_basic_data", "抓取其他基础数据（龙虎榜、资金流向等）")
    basic_data_other_daily_job.main()

    # 任务1.5：修复除权股票的历史K线字段
    # cn_stock_bonus 在其他基础数据任务中更新；技术指标、K线形态和策略都依赖修复后的历史K线。
    log_task_start("adjustment_kline_repair", "修复除权股票前复权K线数据")
    run_adjustment_kline_repair_for_daily_job()

    # 任务2：计算技术指标（MACD、KDJ、RSI等）
    log_task_start("calculate_indicators", "计算股票技术指标")
    indicators_data_daily_job.main()  # 依赖：历史K线数据

    # 任务3：识别K线形态（早晨之星、黄昏之星等）
    log_task_start("recognize_kline_patterns", "识别K线形态")
    klinepattern_data_daily_job.main()  # 依赖：历史K线数据

    # 任务4：策略选股（多因子策略、技术形态策略等）
    log_task_start("strategy_selection", "执行策略选股")
    strategy_data_daily_job.main()  # 依赖：历史K线数据

    # 步骤4：回测验证
    # 【重要】直接调用prepare(None)，回测所有历史待回测记录
    # 而不是通过run_with_args获取今天的日期（会导致只回测当天）
    from instock.job import backtest_data_daily_job
    backtest_data_daily_job.prepare(None)  # None表示回测所有待回测的历史记录

    # 步骤5：收盘后数据（资金流向、分红配送、龙虎榜等）
    log_task_start("after_close_data", "抓取收盘后数据（资金流向、龙虎榜等）")
    basic_data_after_close_daily_job.main()
    
    # ==================== 记录任务完成 ====================
    # 计算总耗时
    elapsed_time = time.time() - start
    
    # 记录完成日志
    logging.info("######## 完成任务, 使用时间: %s 秒 #######" % elapsed_time)
    logging.info("")
    
    # 在终端也打印完成信息（方便用户看到）
    print(f"任务完成！总耗时：{elapsed_time:.2f}秒")
    print(f"日志文件：{os.path.join(log_path, 'stock_execute_job.log')}")

    # ==================== 验证数据写入 ====================
    # 验证指定日期的数据是否真的写入数据库。
    # data_validation_daily_job 会复用当前命令行日期参数：
    # 无参数验证最新交易日；单日/多日/区间则验证对应日期。
    data_validation_daily_job.main()


def main_calculate_only():
    """
    仅执行计算任务（不包含初始化和基础数据抓取）
    
    适用场景：
    - 基础数据已存在，只需重新计算技术指标、K线形态和策略选股
    - 快速验证计算逻辑，跳过耗时的数据抓取步骤
    
    使用方法：
    - python execute_daily_job.py --calculate-only                    # 使用最新交易日
    - python execute_daily_job.py --calculate-only 2026-05-14         # 指定单个日期
    - python execute_daily_job.py --calculate-only 2026-05-10,2026-05-14  # 多个日期
    - python execute_daily_job.py --calculate-only 2026-05-10 2026-05-14  # 日期区间
    
    注意：所有子任务（K线形态识别、策略选股、回测验证）都支持日期参数
    """
    # 导入任务工具
    from instock.job.task_utils import log_task_start
    
    # 任务开始日志
    log_task_start("calculate_only_mode", "仅执行计算任务模式（跳过初始化和基础数据抓取）")

    # 先修复除权股票K线，确保后续并行计算读取到一致的历史价格。
    run_adjustment_kline_repair_for_daily_job()
    
    # ==================== 步骤3：并行执行多个任务 ====================
    # 这些任务相互独立，可以同时执行，提高效率
    with concurrent.futures.ThreadPoolExecutor() as executor:

        # 任务2：计算技术指标
        executor.submit(indicators_data_daily_job.main)  # 依赖：历史K线数据

        # 任务3：识别K线形态
        executor.submit(klinepattern_data_daily_job.main)  # 依赖：历史K线数据（与任务2并行）

        # 任务4：策略选股
        executor.submit(strategy_data_daily_job.main)  # 依赖：历史K线数据（与任务2、3并行）

    # 步骤4：回测验证
    # 【重要】直接调用prepare(None)，回测所有历史待回测记录
    # 而不是通过run_with_args获取今天的日期（会导致只回测当天）
    from instock.job import backtest_data_daily_job
    backtest_data_daily_job.prepare(None)  # None表示回测所有待回测的历史记录

# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    程序入口点 - 支持命令行参数选择执行模式
    
    Python程序执行流程：
    1. 当直接运行这个脚本时，__name__ == '__main__'
    2. 当被其他模块导入时，__name__ == 模块名
    3. 这个判断确保只有直接运行时才执行main()
    
    为什么这样设计？
    - 模块可以被导入而不自动执行
    - 可以在其他地方调用main()函数
    - 符合Python的最佳实践
    
    支持的命令行参数：
    - 无参数：执行完整流程（main）
    - --calculate-only：仅执行计算任务（main_calculate_only）
    
    使用示例：
    python execute_daily_job.py                    # 执行完整流程
    python execute_daily_job.py --calculate-only   # 仅执行计算任务
    """
    import sys
    
    # 解析命令行参数
    calculate_only = '--calculate-only' in sys.argv
    # 【关键修复】移除控制参数，避免传递给 run_template 后被误判为日期参数
    sys.argv = [arg for arg in sys.argv if arg != '--calculate-only']

    if calculate_only:
        logging.info("📝 检测到 --calculate-only 参数，执行仅计算模式")
        main_calculate_only()
    else:
        logging.info("📝 执行完整流程模式")
        main()
