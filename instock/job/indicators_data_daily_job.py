#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
技术指标计算任务模块 - 75种指标批量计算与信号筛选
==================================================

功能说明：
本模块负责批量计算所有股票的技术指标，并筛选出买入/卖出信号。
是系统数据分析的核心环节，为策略选股和回测提供数据基础。

核心职责：
1. 批量获取股票历史K线数据
2. 并行计算75种技术指标
3. 识别超买超卖信号
4. 保存计算结果到数据库
5. 性能优化（多线程+缓存）

什么是技术指标？
技术指标是通过数学公式对价格、成交量等数据进行加工得到的分析工具。

指标分类：

一、趋势类指标（6种）
-------------------
用于判断市场趋势方向和强度

1. MACD（指数平滑异同移动平均线）
   - 原理：快慢均线的差值和平均值
   - 用法：金叉买入，死叉卖出
   - 特点：滞后但稳定，适合中长线

2. PPO（价格震荡百分比指标）
   - 原理：类似MACD，但用百分比表示
   - 用法：比较不同价格股票的动量
   - 特点：标准化，便于横向对比

3. TRIX（三重指数平滑移动平均）
   - 原理：三次平滑处理
   - 用法：过滤短期波动，看长期趋势
   - 特点：非常平滑，适合长线

4. DMI（趋向指标系统）
   - 包含：PDI、MDI、ADX、ADXR
   - 用法：ADX>25表示趋势强劲
   - 特点：判断趋势强度的最佳指标

5. SAR（抛物线转向指标）
   - 原理：抛物线轨迹追踪
   - 用法：设置止损点
   - 特点：简单直观，适合趋势交易

6. SUPERTREND（超级趋势）
   - 原理：基于ATR的动态通道
   - 用法：线上持股，线下持币
   - 特点：结合波动率，更智能

二、摆动类指标（7种）
-------------------
用于判断超买超卖和转折点

7. KDJ（随机指标）
   - 原理：收盘价在周期内的相对位置
   - 用法：KDJ<20超卖，>80超买
   - 特点：灵敏但易产生假信号

8. RSI（相对强弱指标）
   - 原理：上涨力度vs下跌幅度
   - 用法：RSI<30超卖，>70超买
   - 特点：经典指标，广泛应用

9. STOCHRSI（随机RSI）
   - 原理：RSI的随机版本
   - 用法：更灵敏的超买超卖
   - 特点：波动剧烈，适合短线

10. W&R（威廉指标）
    - 原理：最高价与当前价的差距
    - 用法：W&R<-80超卖，>-20超买
    - 特点：与RSI相反，范围0到-100

11. CCI（顺势指标）
    - 原理：价格偏离统计平均的程度
    - 用法：CCI<-100超卖，>100超买
    - 特点：无上下限，捕捉极端行情

12. PSY（心理线指标）
    - 原理：上涨天数占总天数的比例
    - 用法：PSY<25超卖，>75超买
    - 特点：衡量市场情绪

13. BIAS（乖离率）
    - 原理：价格偏离均线的程度
    - 用法：负乖离过大可能反弹
    - 特点：简单有效

三、通道类指标（2种）
-------------------
用于判断价格波动的上下边界

14. BOLL（布林带）
    - 原理：均线±2倍标准差
    - 用法：触及下轨买入，上轨卖出
    - 特点：收口预示变盘，开口预示趋势

15. ENE（轨道线）
    - 原理：类似布林线，算法不同
    - 用法：轨道内震荡，突破跟随
    - 特点：更适合震荡市

四、量价类指标（8种）
-------------------
结合价格和成交量信息

16. CR（能量指标）
    - 原理：考虑昨日价格的动量
    - 用法：CR<40超卖，>300超买
    - 特点：比BRAR更稳定

17. VR（成交量比率）
    - 原理：上涨成交量vs下跌成交量
    - 用法：VR<40超卖，>160超买
    - 特点：量在价先

18. OBV（能量潮）
    - 原理：累积成交量
    - 用法：OBV领先价格
    - 特点：判断资金流向

19. MFI（资金流量指标）
    - 原理：RSI的成交量版本
    - 用法：MFI<20超卖，>80超买
    - 特点：结合价格和成交量

20. EMV（简易波动指标）
    - 原理：价格变动与成交量的关系
    - 用法：EMV上升表示轻松上涨
    - 特点：衡量上涨难度

21. VWMA（成交量加权均线）
    - 原理：成交量加权的移动平均
    - 用法：大成交量日子权重更大
    - 特点：反映真实成本

22. FI（劲道指数）
    - 原理：价格变化×成交量
    - 用法：衡量价格变动的力度
    - 特点：简单直观

23. DMA（平行线差）
    - 原理：两条均线的差值
    - 用法：DMA>0多头，<0空头
    - 特点：简单有效的趋势指标

五、其他类指标（9种）
-------------------

24. TEMA（三重指数移动平均）
    - 原理：减少滞后的移动平均
    - 用法：对近期价格更敏感
    - 特点：反应快速

25. RVI（相对离散指数）
    - 原理：类似MACD的oscillator
    - 用法：判断趋势方向
    - 特点：计算方法独特

26. WT（Wave Trend）
    - 原理：LazyBear开发的趋势指标
    - 用法：TradingView流行指标
    - 特点：社区认可度高

27. ROC（变动率）
    - 原理：价格变化的百分比
    - 用法：衡量变化速度
    - 特点：简单明了

28. VHF（十字过滤线）
    - 原理：判断趋势还是震荡
    - 用法：VHF高=趋势，低=震荡
    - 特点：选择策略的依据

29. ATR（真实波幅）
    - 原理：价格波动的平均值
    - 用法：设置止损距离
    - 特点：风险管理必备

30. BRAR（人气意愿指标）
    - 原理：买卖意愿的强度
    - 用法：BR>150过热，<50过冷
    - 特点：台湾股市常用

31. DPO（区间震荡线）
    - 原理：消除长期趋势
    - 用法：识别周期高低点
    - 特点：适合波段操作

32. VHF补充和其他变种

买卖信号规则：

超卖信号（买入机会）：
```python
buy_signals = {
    'KDJ': < 20,      # 随机指标超卖
    'RSI': < 20,      # 相对强弱超卖
    'CCI': < -100,    # 顺势指标超卖
    'CR': < 40,       # 能量指标超卖
    'W&R': < -80,     # 威廉指标超卖
    'VR': < 40,       # 成交量比率超卖
}
```

超买信号（卖出风险）：
```python
sell_signals = {
    'KDJ': > 80,      # 随机指标超买
    'RSI': > 80,      # 相对强弱超买
    'CCI': > 100,     # 顺势指标超买
    'CR': > 300,      # 能量指标超买
    'W&R': > -20,     # 威廉指标超买
    'VR': > 160,      # 成交量比率超买
}
```

数据流程：
┌─────────────────┐
│ 基础股票列表     │
└────────┬────────┘
         ↓
┌─────────────────┐
│ 获取历史K线数据  │ ← 从单例缓存读取
└────────┬────────┘
         ↓
┌─────────────────┐
│ 并行计算75种指标 │ ← ThreadPoolExecutor
└────────┬────────┘
         ↓
┌─────────────────┐
│ 筛选买卖信号     │ ← 根据阈值判断
└────────┬────────┘
         ↓
┌─────────────────┐
│ 保存到数据库     │ ← 批量插入
└─────────────────┘

执行流程详解：

1. 数据准备阶段：
   - 从stock_hist_data单例获取所有股票的历史数据
   - 单例模式确保数据只加载一次，提高性能
   - 数据已在前置任务中抓取并缓存

2. 指标计算阶段：
   - 使用ThreadPoolExecutor并行计算
   - 每只股票独立计算，互不影响
   - 调用calculate_indicator模块的get_indicators()
   - 一次性计算75种指标（避免重复计算）

3. 信号筛选阶段：
   - 遍历每个指标的当前值
   - 对比超买超卖阈值
   - 标记符合条件的股票
   - 生成买入/卖出信号列表

4. 数据保存阶段：
   - 删除该日期的旧数据（避免重复）
   - 批量插入新计算的指标数据
   - 使用insert_db_from_df()高效写入
   - 记录成功/失败数量

性能优化：

1. 并行计算：
```python
# 使用线程池并行处理
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(calc_one_stock, code) for code in codes]
    results = [f.result() for f in concurrent.futures.as_completed(futures)]
```

2. 单例缓存：
```python
# 历史数据只加载一次
stock_data = stock_hist_data(date=date).get_data()
```

3. 批量插入：
```python
# DataFrame批量写入，比逐条快10倍+
mdb.insert_db_from_df(df, table_name)
```

4. 内存管理：
```python
# 及时释放不需要的变量
del temp_df
gc.collect()
```

运行时机：
- 必须在basic_data_daily_job之后（需要历史数据）
- 建议在收盘后执行（17:30之后）
- 可以与其他任务并行（basic_data_other、klinepattern、strategy）

数据量估算：
- 股票数量：约4000-5000只
- 每只股票：约360天历史数据
- 指标数量：75种
- 计算时间：5-10分钟（8线程并行）
- 存储空间：约50-100MB/天

使用示例：

单独运行：
```bash
python indicators_data_daily_job.py 2024-01-01
```

作为调度的一部分：
```python
# 在execute_daily_job.py中被调用
indicators_data_daily_job.execute(job_run_date)
```

查询结果：
```sql
-- 查看某天的指标数据
SELECT * FROM stock_indicators 
WHERE date = '2024-01-01' 
LIMIT 10;

-- 查找超卖股票
SELECT code, name, kdjk, rsi_6, cci 
FROM stock_indicators 
WHERE date = '2024-01-01'
  AND kdjk < 20 
  AND rsi_6 < 20;
```

注意事项：

1. 依赖条件：
   - 必须先执行basic_data_daily_job
   - 数据库连接正常
   - 足够的内存（建议4GB+）

2. 性能调优：
   - 调整max_workers（根据CPU核心数）
   - 监控内存使用
   - 必要时分批处理

3. 常见问题：
   - 内存溢出：减少并发数或分批处理
   - 计算缓慢：检查CPU负载
   - 数据库超时：增加timeout参数
   - 数据缺失：检查前置任务

4. 验证方法：
```python
# 检查是否所有股票都计算了
SELECT COUNT(*) FROM stock_indicators WHERE date = '2024-01-01';
-- 应该接近股票总数

# 检查指标是否正常
SELECT AVG(kdjk), AVG(rsi_6), AVG(macd) 
FROM stock_indicators 
WHERE date = '2024-01-01';
-- 应该在合理范围内
```

依赖关系：
- instock.lib.run_template：任务运行模板
- instock.core.tablestructure：表结构定义
- instock.lib.database：数据库操作
- instock.core.indicator.calculate_indicator：指标计算核心
- instock.core.singleton_stock：历史数据单例

最佳实践：
1. 定期清理历史数据（保留1-2年）
2. 监控计算时间和成功率
3. 设置失败重试机制
4. 记录异常股票（计算失败的）
5. 建立指标字典文档
6. 定期验证指标准确性
"""

# ==================== 导入必需的库 ====================
import logging  # logging：日志记录，跟踪任务执行情况
import concurrent.futures  # concurrent.futures：多线程并发，加速指标计算
import pandas as pd  # pandas：数据处理，DataFrame操作
import os.path  # os.path：路径操作，构建文件路径
import sys  # sys：系统操作，添加Python路径

# ==================== 路径配置 ====================
# 将项目根目录添加到Python路径
cpath_current = os.path.dirname(os.path.dirname(__file__))  # instock目录
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))  # 项目根目录
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板，提供统一的执行框架
import instock.core.tablestructure as tbs  # 表结构定义，包含表名和字段信息
import instock.lib.database as mdb  # 数据库操作，提供插入和查询功能
import instock.core.indicator.calculate_indicator as idr  # 指标计算模块，核心算法实现
from instock.core.singleton_stock import stock_hist_data  # 历史数据单例，缓存K线数据

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 准备并计算指标 ====================

"""
准备并计算所有股票的技术指标
参数说明：
date (datetime.date): 计算日期
功能说明：
1. 获取所有股票的历史K线数据
2. 并行计算每只股票的75种技术指标
3. 合并结果并保存到数据库
执行流程：
1. 从单例获取历史数据（已缓存）
2. 并行计算指标（多线程）
3. 构建结果DataFrame
4. 删除旧数据
5. 插入新数据
数据量：
- 股票数量：约4000只
- 指标数量：75种
- 计算时间：约5-10分钟（多线程）
为什么需要历史数据？
- 技术指标需要一定周期的数据
- 如MA20需要20天数据
- 如MA250需要250天数据
- 默认获取3年历史数据
"""
def prepare(date):
    try:
        # 步骤1: 获取所有股票的历史K线数据
        # stock_hist_data()：单例模式，只加载一次
        # get_data()：返回字典 {(date, code, name): DataFrame}
        stocks_data = stock_hist_data(date=date).get_data()
        
        # 检查数据是否有效
        if stocks_data is None:
            # 没有历史数据，无法计算指标
            logging.warning(f"没有历史数据，无法计算指标：{date}")
            return
        
        # 步骤2: 并行计算所有股票的指标
        # run_check()：使用多线程计算
        # 返回字典：{(date, code, name): Series(指标数据)}
        results = run_check(stocks_data, date=date)
        
        if results is None:
            # 计算失败
            logging.warning(f"指标计算失败：{date}")
            return

        # 步骤3: 获取表名
        table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']  # 'cn_stock_indicators'
        
        # 步骤4: 删除旧数据（如果表存在）
        if mdb.checkTableIsExist(table_name):
            # 表存在，删除该日期的旧数据
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            # 表不存在，获取字段类型
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS['columns'])

        # 步骤5: 构建DataFrame
        # results.keys()：所有股票的(date, code, name)
        dataKey = pd.DataFrame(results.keys())
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        dataKey.columns = _columns  # 设置列名：date, code, name
        
        # results.values()：所有股票的指标数据（Series）
        dataVal = pd.DataFrame(results.values())
        
        # 删除date列（因为dataKey已经有了）
        # axis=1：按列删除
        # inplace=True：直接修改原DataFrame
        dataVal.drop('date', axis=1, inplace=True)

        # 步骤6: 合并两个DataFrame
        # merge()：类似SQL的JOIN
        # on=['code']：按code列合并
        # how='left'：左连接，保留左边所有行
        data = pd.merge(dataKey, dataVal, on=['code'], how='left')
        
        # 步骤7: 日期处理
        # 确保date列是正确的日期（单例模式下可能有问题）
        date_str = date.strftime("%Y-%m-%d")
        if date.strftime("%Y-%m-%d") != data.iloc[0]['date']:
            # 如果日期不匹配，更新为正确的日期
            data['date'] = date_str
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (技术指标)")
        logging.info(f"   目标日期: {date}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        # 步骤8: 插入数据到数据库
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"指标计算完成：{date}，共{len(data)}只股票")

    except Exception as e:
        logging.error(f"indicators_data_daily_job.prepare处理异常：{e}")


# ==================== 并行计算指标 ====================

"""
并行计算多只股票的技术指标
参数说明：
stocks (dict): 股票历史数据字典
- 键：(date, code, name)
- 值：DataFrame（历史K线）
date (datetime.date): 计算日期
workers (int): 线程池大小，默认40
返回值：
dict: 计算结果字典
- 键：(date, code, name)
- 值：Series（指标数据）
并行计算原理：
- 单线程：4000只股票串行计算，很慢
- 多线程：40个线程同时计算，快很多
- CPU密集型：主要是数学计算
执行流程：
1. 构建列名列表
2. 创建线程池
3. 提交所有计算任务
4. 等待完成并收集结果
5. 返回结果字典
"""
def run_check(stocks, date=None, workers=40):
    # 步骤1: 准备存储结果的字典
    data = {}
    
    # 步骤2: 构建列名列表
    # STOCK_STATS_DATA：指标数据结构定义
    columns = list(tbs.STOCK_STATS_DATA['columns'])
    columns.insert(0, 'code')  # 在开头插入code
    columns.insert(0, 'date')  # 在开头插入date
    data_column = columns  # 列名列表
    
    total_stocks = len(stocks)
    logging.info(f"🔢 开始计算技术指标，共 {total_stocks} 只股票，使用 {workers} 个线程")
    
    try:
        # 步骤3: 创建线程池
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            
            # 步骤4: 为每只股票提交计算任务
            # 字典推导式：{Future: 股票键}
            future_to_data = {
                # executor.submit()：提交任务
                # idr.get_indicator：指标计算函数
                # k：股票键(date, code, name)
                # stocks[k]：该股票的历史K线DataFrame
                # data_column：列名列表
                # date：计算日期
                executor.submit(idr.get_indicator, k, stocks[k], data_column, date=date): k 
                for k in stocks  # 遍历所有股票
            }
            
            # 步骤5: 等待任务完成并收集结果
            completed_count = 0
            for future in concurrent.futures.as_completed(future_to_data):
                stock = future_to_data[future]  # 获取对应的股票键
                try:
                    _data_ = future.result()  # 获取计算结果
                    if _data_ is not None:
                        data[stock] = _data_  # 存储结果
                    
                    # 更新进度
                    completed_count += 1
                    if completed_count % 500 == 0 or completed_count == total_stocks:
                        progress = (completed_count / total_stocks) * 100
                        logging.info(f"📊 指标计算进度: {completed_count}/{total_stocks} ({progress:.1f}%)")
                        
                except Exception as e:
                    # 单只股票计算失败，记录日志
                    logging.error(f"indicators_data_daily_job.run_check处理异常：{stock[1]}代码{e}")
                    completed_count += 1
                    
    except Exception as e:
        # 整体执行异常
        logging.error(f"indicators_data_daily_job.run_check处理异常：{e}")
    
    logging.info(f"✅ 技术指标计算完成，成功计算 {len(data)} 只股票")
    
    # 步骤6: 检查结果并返回
    if not data:
        return None
    else:
        return data


# ==================== 筛选买入信号 ====================

"""
根据技术指标筛选买入信号（超卖区域）
参数说明：
date (datetime.date): 筛选日期
功能说明：
从已计算的指标中筛选符合买入条件的股票
买入条件（所有条件同时满足）：
1. KDJ_K >= 80：K值进入超买区
2. KDJ_D >= 70：D值进入超买区
3. KDJ_J >= 100：J值超过100
4. RSI_6 >= 80：6日RSI超买
5. CCI >= 100：CCI进入超买区
6. CR >= 300：CR能量指标高位
7. WR_6 >= -20：威廉指标超买
8. VR >= 160：成交量比率高位
为什么这样设置？
- 多个指标共振：提高准确率
- 超买区域：股价可能回调
- 短线交易：寻找高位卖出机会
注意：
- 这是简单筛选策略
- 实际应用需结合其他因素
- 回测验证策略有效性
数据流程：
指标数据 → SQL筛选 → 买入信号表 → 回测验证
"""
def guess_buy(date):
    try:
        # 步骤1: 获取指标表名
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        
        # 步骤2: 检查表是否存在
        if not mdb.checkTableIsExist(_table_name):
            # 表不存在，说明还没计算指标
            logging.warning(f"指标表不存在，无法筛选买入信号：{date}")
            return

        # 步骤3: 构建查询的列名
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])  # date, code, name
        _selcol = '`,`'.join(_columns)  # 用反引号和逗号连接：`date`,`code`,`name`
        
        # 步骤4: 构建SQL查询语句
        # 筛选条件：所有指标都达到超买阈值
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and 
                `kdjk` >= 80 and `kdjd` >= 70 and `kdjj` >= 100 and `rsi_6` >= 80 and 
                `cci` >= 100 and `cr` >= 300 and `wr_6` >= -20 and `vr` >= 160'''
        
        # 步骤5: 执行查询
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        # 步骤6: 去重（按code）
        # subset="code"：根据code列判断重复
        # keep="last"：保留最后一个
        data = data.drop_duplicates(subset="code", keep="last")
        
        # 检查是否有结果
        if len(data.index) == 0:
            # 没有符合条件的股票
            logging.info(f"没有买入信号：{date}")
            return

        # 步骤7: 准备保存到买入信号表
        table_name = tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']  # 'cn_stock_indicators_buy'
        
        # 删除旧数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns'])

        # 步骤8: 添加回测数据列（空值，待回测填充）
        # TABLE_CN_STOCK_BACKTEST_DATA：回测数据列定义
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        # concat()：连接DataFrame
        # pd.DataFrame(columns=...)：创建空DataFrame（只有列名）
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (买入信号)")
        logging.info(f"   目标日期: {date}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        # 步骤9: 插入数据到买入信号表
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"买入信号筛选完成：{date}，共{len(data)}只股票")
        
    except Exception as e:
        logging.error(f"indicators_data_daily_job.guess_buy处理异常：{e}")


# ==================== 筛选卖出信号 ====================

"""
根据技术指标筛选卖出信号（超卖区域）
参数说明：
date (datetime.date): 筛选日期
功能说明：
从已计算的指标中筛选符合卖出条件的股票
卖出条件（所有条件同时满足）：
1. KDJ_K < 20：K值进入超卖区
2. KDJ_D < 30：D值进入超卖区
3. KDJ_J < 10：J值低于10
4. RSI_6 < 20：6日RSI超卖
5. CCI < -100：CCI进入超卖区
6. CR < 40：CR能量指标低位
7. WR_6 < -80：威廉指标超卖
8. VR < 40：成交量比率低位
为什么这样设置？
- 超卖区域：股价可能反弹
- 多指标共振：提高准确率
- 短线交易：寻找低位买入机会
与guess_buy的区别：
- 买入信号：超买区域（高位）
- 卖出信号：超卖区域（低位）
- 策略相反
数据流程：
指标数据 → SQL筛选 → 卖出信号表 → 回测验证
"""
def guess_sell(date):
    try:
        # 步骤1: 获取指标表名
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        
        # 步骤2: 检查表是否存在
        if not mdb.checkTableIsExist(_table_name):
            logging.warning(f"指标表不存在，无法筛选卖出信号：{date}")
            return

        # 步骤3: 构建查询列名
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)
        
        # 步骤4: 构建SQL查询语句（超卖条件）
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and 
                `kdjk` < 20 and `kdjd` < 30 and `kdjj` < 10 and `rsi_6` < 20 and 
                `cci` < -100 and `cr` < 40 and `wr_6` < -80 and `vr` < 40'''
        
        # 步骤5: 执行查询
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        # 步骤6: 去重
        data = data.drop_duplicates(subset="code", keep="last")
        
        # 检查结果
        if len(data.index) == 0:
            logging.info(f"没有卖出信号：{date}")
            return

        # 步骤7: 准备保存到卖出信号表
        table_name = tbs.TABLE_CN_STOCK_INDICATORS_SELL['name']  # 'cn_stock_indicators_sell'
        
        # 删除旧数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            logging.info(f"🗑️  已删除 {table_name} 表中 {date} 的旧数据")
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS_SELL['columns'])
            logging.info(f"📋 表 {table_name} 不存在，将创建新表")
        
        # 步骤8: 添加回测数据列
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # 准备插入数据
        logging.info(f"💾 准备插入数据到表: {table_name} (卖出信号)")
        logging.info(f"   目标日期: {date}")
        logging.info(f"   数据量: {len(data)}条记录")
        logging.info(f"   开始插入数据...")
        
        # 步骤9: 插入数据
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"卖出信号筛选完成：{date}，共{len(data)}只股票")
        
    except Exception as e:
        logging.error(f"indicators_data_daily_job.guess_sell处理异常：{e}")


# ==================== 主函数 ====================

"""
技术指标任务主函数
功能说明：
按顺序执行三个任务：
1. 计算所有股票的技术指标
2. 筛选买入信号（超买）
3. 筛选卖出信号（超卖）
执行顺序：
prepare() → guess_buy() → guess_sell()
必须按顺序，因为后面依赖前面的结果
运行方式：
# 计算今天的指标
python indicators_data_daily_job.py
# 计算指定日期
python indicators_data_daily_job.py 2024-01-01
# 批量计算
python indicators_data_daily_job.py 2024-01-01 2024-01-31
运行时机：
- 收盘后：15:00后
- 建议：17:30后（确保基础数据完整）
数据用途：
- 技术分析：查看指标值
- 选股：根据指标筛选
- 回测：验证指标策略
- Web展示：显示指标图表
"""
def main():
    # 导入任务工具
    from instock.job.task_utils import log_task_start
    
    # 任务1: 计算技术指标
    log_task_start("indicators_calculation", "批量计算75种技术指标并筛选买卖信号")
    # run_with_args()：处理命令行参数，调用prepare()
    runt.run_with_args(prepare)
    
    # 任务2: 筛选买入信号
    log_task_start("buy_signal_filtering", "筛选买入信号股票")
    runt.run_with_args(guess_buy)
    
    # 任务3: 筛选卖出信号
    log_task_start("sell_signal_filtering", "筛选卖出信号股票")
    runt.run_with_args(guess_sell)
    
    logging.info("技术指标任务执行完成")


# ==================== 程序入口 ====================
# main函数入口
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    前置条件：
        1. 已有基础数据（basic_data_daily_job）
        2. 已有历史K线（stock_hist_data）
        
    产生数据：
        1. cn_stock_indicators：所有股票的75种指标
        2. cn_stock_indicators_buy：买入信号股票
        3. cn_stock_indicators_sell：卖出信号股票
        
    后续任务：
        - backtest_data_daily_job：回测验证
        - Web展示：显示指标和信号
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
技术指标任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第三层：技术指标层
   - 依赖：基础数据、历史K线
   - 产出：75种技术指标、买卖信号

2. 核心概念
   技术指标：
   - 数学公式计算的分析工具
   - 基于价格和成交量
   - 帮助判断买卖时机
   
   超买超卖：
   - 超买：价格上涨过度，可能回调
   - 超卖：价格下跌过度，可能反弹

3. 75种指标
   趋势类：MACD、DMI、DMA
   摆动类：KDJ、RSI、CCI、WR
   通道类：BOLL、ENE
   能量类：CR、VR、OBV
   波动类：ATR、STOCHRSI
   ... 等

4. 买入卖出信号
   买入信号（超买）：
   - 多个指标同时超买
   - 适合高位止盈
   
   卖出信号（超卖）：
   - 多个指标同时超卖
   - 适合低位建仓

5. 并行计算
   - 4000只股票
   - 75种指标
   - 40个线程同时计算
   - 约5-10分钟完成

6. 数据流程
   基础数据 → 历史K线 → 
   计算指标 → 筛选信号 → 
   回测验证 → Web展示

7. SQL筛选
   SELECT date, code, name 
   FROM cn_stock_indicators 
   WHERE date = '2024-01-01' 
   AND kdjk >= 80 
   AND kdjd >= 70 
   ...
   
   解释：
   - 所有条件用AND连接
   - 必须同时满足
   - 提高准确率

8. 回测数据列
   - rate_1, rate_3, rate_5, ...
   - 初始为NULL
   - 待回测任务填充
   - 验证策略有效性

9. 使用场景
   - 技术分析：查看指标图表
   - 量化选股：根据指标筛选
   - 策略回测：验证指标策略
   - 实盘交易：发现交易机会

10. Python知识点
    - 多线程：concurrent.futures
    - DataFrame操作：pd.merge, pd.concat
    - SQL查询：pd.read_sql
    - 去重：drop_duplicates
    - 数据库操作：增删改查

11. 常见问题
    Q: 指标计算很慢？
    A: 正常，需要处理大量数据
    
    Q: 没有买卖信号？
    A: 可能当天没有符合条件的股票
    
    Q: 指标值为0？
    A: 可能数据不足或计算异常
    
    Q: 如何调整阈值？
    A: 修改guess_buy/guess_sell中的条件

12. 优化建议
    - 增加线程数：根据CPU核心数
    - 缓存历史数据：避免重复加载
    - 只计算必要指标：提高速度
    - 定期清理旧数据：节省空间
"""
