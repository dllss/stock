# cn_stock_indicators_buy 选股逻辑详解

## 📋 目录

- [一、概述](#一概述)
- [二、选股原理](#二选股原理)
- [三、选股条件](#三选股条件)
- [四、指标详解](#四指标详解)
- [五、代码实现](#五代码实现)
- [六、执行流程](#六执行流程)
- [七、策略特点](#七策略特点)
- [八、使用建议](#八使用建议)
- [九、表结构说明](#九表结构说明)
- [十、回测验证](#十回测验证)

---

## 一、概述

**cn_stock_indicators_buy** 是一个基于技术指标超买信号的买入策略选股表。当多个技术指标同时达到超买阈值时，认为股票处于强势状态，系统会自动将其标记为买入信号。

### 核心特点

- **多指标共振**：8个技术指标必须同时满足条件
- **量化标准**：明确的数值阈值，可回测验证
- **自动筛选**：每日收盘后自动计算并更新
- **短线策略**：适合捕捉强势股的短线机会

---

## 二、选股原理

该策略基于**技术指标超买理论**：

> 当多个技术指标同时进入超买区域时，表明股票当前处于强势上涨状态，买方力量占据主导地位。虽然超买区域通常被认为是风险区域，但在强势市场中，超买可能持续较长时间，形成"强者恒强"的效应。

### 策略逻辑

1. **超买确认**：通过8个不同维度的指标确认超买状态
2. **多指标共振**：单一指标可能误判，多指标同时超买提高准确性
3. **强势股筛选**：只选择最强势的股票，放弃温和上涨的股票
4. **短线交易**：适合短线追涨，需要快速止盈止损

---

## 三、选股条件

### SQL查询语句

```sql
SELECT `date`, `code`, `name` 
FROM `cn_stock_indicators` 
WHERE `date` = '{date}' 
  AND `kdjk` >= 80      -- KDJ的K值 ≥ 80
  AND `kdjd` >= 70      -- KDJ的D值 ≥ 70
  AND `kdjj` >= 100     -- KDJ的J值 ≥ 100
  AND `rsi_6` >= 80     -- 6日RSI ≥ 80
  AND `cci` >= 100      -- CCI ≥ 100
  AND `cr` >= 300       -- CR能量指标 ≥ 300
  AND `wr_6` >= -20     -- 6日威廉指标 ≥ -20
  AND `vr` >= 160       -- VR成交量比率 ≥ 160
```

### 条件说明

所有8个条件必须**同时满足**（AND连接），缺一不可。这是一个非常严格的筛选条件，确保选出的股票确实处于极度强势状态。

---

## 四、指标详解

### 1. KDJ随机指标（3个条件）

| 指标 | 阈值 | 含义 |
|------|------|------|
| **KDJ_K** | ≥ 80 | K值进入超买区，表示股价强势上涨 |
| **KDJ_D** | ≥ 70 | D值确认超买趋势，过滤假信号 |
| **KDJ_J** | ≥ 100 | J值极度超买，短期动能极强 |

**KDJ指标说明**：
- KDJ是最常用的摆动指标之一
- K值和D值在0-100之间波动
- J值可以超过100或低于0，反映极端情况
- 当K、D、J同时高位运行时，表明股票处于强势状态

### 2. RSI相对强弱指标

| 指标 | 阈值 | 含义 |
|------|------|------|
| **RSI_6** | ≥ 80 | 6日RSI进入超买区，短期买方力量极强 |

**RSI指标说明**：
- RSI衡量价格变动的速度和幅度
- 取值范围0-100
- RSI > 70为超买区，RSI < 30为超卖区
- 6日RSI对短期价格变化更敏感

### 3. CCI顺势指标

| 指标 | 阈值 | 含义 |
|------|------|------|
| **CCI** | ≥ 100 | 价格偏离均线，处于超买区域 |

**CCI指标说明**：
- CCI测量价格偏离统计平均值的程度
- 正常范围-100到+100
- CCI > 100表示价格异常强势
- 适合捕捉突破行情

### 4. CR能量指标

| 指标 | 阈值 | 含义 |
|------|------|------|
| **CR** | ≥ 300 | 多头能量极度旺盛，买盘强劲 |

**CR指标说明**：
- CR衡量多空双方的能量对比
- 没有固定上下限
- CR > 300表示多头能量极强
- 适合判断强势股的持续性

### 5. WR威廉指标

| 指标 | 阈值 | 含义 |
|------|------|------|
| **WR_6** | ≥ -20 | 接近0轴，处于超买区域 |

**WR指标说明**：
- WR取值范围-100到0
- WR > -20为超买区
- WR < -80为超卖区
- 6日WR对短期变化敏感

### 6. VR成交量比率

| 指标 | 阈值 | 含义 |
|------|------|------|
| **VR** | ≥ 160 | 成交量放大，资金活跃度高 |

**VR指标说明**：
- VR衡量上涨成交量与下跌成交量的比率
- VR > 150表示成交量配合价格上涨
- VR > 160确认资金积极介入
- 成交量是价格的重要支撑

---

## 五、代码实现

### 文件位置

**主文件**：`instock/job/indicators_data_daily_job.py`

**函数**：`guess_buy(date)` （第634-698行）

### 核心代码

```python
def guess_buy(date):
    try:
        # 步骤1: 获取指标表名
        _table_name = tbs.TABLE_CN_STOCK_INDICATORS['name']
        
        # 步骤2: 检查表是否存在
        if not mdb.checkTableIsExist(_table_name):
            logging.warning(f"指标表不存在，无法筛选买入信号：{date}")
            return

        # 步骤3: 构建查询列名
        _columns = tuple(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])
        _selcol = '`,`'.join(_columns)
        
        # 步骤4: 构建SQL查询语句（8个指标条件）
        sql = f'''SELECT `{_selcol}` FROM `{_table_name}` WHERE `date` = '{date}' and 
                `kdjk` >= 80 and `kdjd` >= 70 and `kdjj` >= 100 and `rsi_6` >= 80 and 
                `cci` >= 100 and `cr` >= 300 and `wr_6` >= -20 and `vr` >= 160'''
        
        # 步骤5: 执行查询
        data = pd.read_sql(sql=sql, con=mdb.engine())
        
        # 步骤6: 去重（按code）
        data = data.drop_duplicates(subset="code", keep="last")
        
        # 检查是否有结果
        if len(data.index) == 0:
            logging.info(f"没有买入信号：{date}")
            return

        # 步骤7: 准备保存到买入信号表
        table_name = tbs.TABLE_CN_STOCK_INDICATORS_BUY['name']
        
        # 删除旧数据
        if mdb.checkTableIsExist(table_name):
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
            mdb.executeSql(del_sql)
            cols_type = None
        else:
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_INDICATORS_BUY['columns'])

        # 步骤8: 添加回测数据列（空值，待回测填充）
        _columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
        data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
        
        # 步骤9: 插入数据到买入信号表
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        logging.info(f"买入信号筛选完成：{date}，共{len(data)}只股票")
        
    except Exception as e:
        logging.error(f"indicators_data_daily_job.guess_buy处理异常：{e}")
```

### 执行时机

在每日任务调度中，`guess_buy()` 在以下时机执行：

```python
# instock/job/execute_daily_job.py

# 任务2：计算技术指标
indicators_data_daily_job.main()  
# ↓ 内部会依次执行：
#   1. prepare() - 计算75种技术指标
#   2. guess_buy() - 筛选买入信号
#   3. guess_sell() - 筛选卖出信号
```

---

## 六、执行流程

### 完整流程图

```
┌─────────────────────────┐
│ 1. 基础数据准备         │
│ - 抓取股票行情数据      │
│ - 计算历史K线           │
└──────────┬──────────────┘
           ↓
┌─────────────────────────┐
│ 2. 计算技术指标         │
│ - 并行计算75种指标      │
│ - 保存到cn_stock_       │
│   indicators表          │
└──────────┬──────────────┘
           ↓
┌─────────────────────────┐
│ 3. SQL筛选买入信号      │
│ - 8个指标同时超买       │
│ - 从indicators表查询    │
└──────────┬──────────────┘
           ↓
┌─────────────────────────┐
│ 4. 数据去重             │
│ - 按股票代码去重        │
│ - 保留最新记录          │
└──────────┬──────────────┘
           ↓
┌─────────────────────────┐
│ 5. 添加回测列           │
│ - rate_1 ~ rate_100     │
│ - 初始值为NULL          │
└──────────┬──────────────┘
           ↓
┌─────────────────────────┐
│ 6. 保存到数据库         │
│ - 删除当日旧数据        │
│ - 插入新数据            │
│ - 表：cn_stock_         │
│   indicators_buy        │
└──────────┬──────────────┘
           ↓
┌─────────────────────────┐
│ 7. 回测验证             │
│ - 计算后续收益率        │
│ - 填充rate_1~rate_100   │
└─────────────────────────┘
```

### 关键步骤说明

#### 步骤1-2：指标计算

由 `prepare()` 函数完成，计算75种技术指标，包括：
- 趋势类：MACD、DMI、DMA
- 摆动类：KDJ、RSI、CCI、WR
- 通道类：BOLL、ENE
- 能量类：CR、VR、OBV
- 波动类：ATR、STOCHRSI
- ...等

#### 步骤3：SQL筛选

使用严格的AND条件，确保8个指标同时达标。

#### 步骤4：去重处理

```python
data = data.drop_duplicates(subset="code", keep="last")
```

防止同一股票出现多次记录。

#### 步骤5：添加回测列

```python
_columns_backtest = tuple(tbs.TABLE_CN_STOCK_BACKTEST_DATA['columns'])
data = pd.concat([data, pd.DataFrame(columns=_columns_backtest)])
```

添加 `rate_1` 到 `rate_100` 共100个收益率字段，初始值为NULL。

#### 步骤6：保存数据

```python
# 删除旧数据
del_sql = f"DELETE FROM `{table_name}` where `date` = '{date}'"
mdb.executeSql(del_sql)

# 插入新数据
mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
```

先删除当日旧数据，再插入新数据，确保数据唯一性。

#### 步骤7：回测验证

由 `backtest_data_daily_job.py` 完成，计算选股后1-100日的收益率。

---

## 七、策略特点

### ✅ 优点

1. **多指标共振**
   - 8个指标同时确认，降低误判率
   - 避免单一指标的局限性
   - 提高选股准确性

2. **量化标准明确**
   - 每个指标都有明确的数值阈值
   - 可回测、可优化
   - 便于统计分析

3. **捕捉强势股**
   - 专门筛选极度强势的股票
   - 适合短线追涨策略
   - 在市场强势期效果好

4. **自动化执行**
   - 每日自动计算和更新
   - 无需人工干预
   - 节省时间成本

5. **完整的回测体系**
   - 提供1-100日收益率数据
   - 可验证策略有效性
   - 支持策略优化

### ⚠️ 风险和局限

1. **高位接盘风险**
   - 超买区域可能是顶部
   - 容易追高被套
   - 需要严格止损

2. **假突破风险**
   - 指标超买但股价不一定继续上涨
   - 可能出现"诱多"陷阱
   - 需要结合其他分析

3. **市场适应性**
   - 在震荡市效果差
   - 只在强势市场有效
   - 熊市中使用风险大

4. **滞后性**
   - 指标基于历史数据
   - 信号发出时可能已错过最佳买点
   - 需要快速决策

5. **过度拟合风险**
   - 阈值可能过度优化
   - 历史表现好不代表未来有效
   - 需要定期重新评估

---

## 八、使用建议

### 1. 配合其他分析

**基本面分析**：
- 查看公司业绩（营收、利润增长）
- 评估估值水平（PE、PB）
- 了解行业前景

**资金面分析**：
- 观察主力资金流向
- 查看龙虎榜数据
- 关注机构持仓变化

**技术面辅助**：
- 查看K线形态
- 分析支撑阻力位
- 观察成交量变化

**消息面关注**：
- 公司公告
- 行业新闻
- 政策变化

### 2. 风险控制

**止损设置**：
```
建议止损位：-5% ~ -8%
严格执行，不抱侥幸心理
```

**仓位管理**：
```
单只股票仓位：不超过总资金的10%
分散投资，降低风险
```

**止盈策略**：
```
短线目标：3% ~ 10%
达到目标及时止盈
不要贪心
```

**时间控制**：
```
持股时间：1-5个交易日
超买信号不宜长期持有
及时获利了结
```

### 3. 市场环境判断

**适合使用的市场**：
- ✅ 牛市初期和中期
- ✅ 强势板块轮动期
- ✅ 成交量放大的市场

**不适合使用的市场**：
- ❌ 熊市或下跌趋势
- ❌ 缩量震荡市
- ❌ 恐慌性下跌时

### 4. 回测验证

**查看收益率数据**：
```sql
-- 查看某日的选股结果和收益率
SELECT date, code, name, rate_1, rate_3, rate_5, rate_10
FROM cn_stock_indicators_buy
WHERE date = '2026-05-20'
ORDER BY rate_1 DESC;
```

**统计胜率**：
```python
# 统计1日收益率的正负比例
positive_rate = (df['rate_1'] > 0).sum() / len(df)
print(f"1日胜率: {positive_rate:.2%}")
```

**优化阈值**：
- 根据回测结果调整指标阈值
- 尝试不同的组合
- 找到最优参数

---

## 九、表结构说明

### cn_stock_indicators_buy 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| **date** | DATE | 选股日期 |
| **code** | VARCHAR(6) | 股票代码 |
| **name** | VARCHAR(20) | 股票名称 |
| **rate_1** | FLOAT | 1日收益率（%） |
| **rate_2** | FLOAT | 2日收益率（%） |
| **...** | ... | ... |
| **rate_100** | FLOAT | 100日收益率（%） |

### 重要说明

⚠️ **这个表不包含技术指标字段！**

很多用户会误以为可以从这个表直接查询KDJ、RSI等指标值，这是错误的。

**正确做法**：
```sql
-- ❌ 错误：这个表没有kdjk字段
SELECT kdjk FROM cn_stock_indicators_buy WHERE code = '000001';

-- ✅ 正确：需要从cn_stock_indicators表查询
SELECT kdjk, rsi_6, cci 
FROM cn_stock_indicators 
WHERE code = '000001' AND date = '2026-05-20';
```

**如果需要同时查看选股结果和技术指标**：
```sql
SELECT 
    buy.date,
    buy.code,
    buy.name,
    ind.kdjk,
    ind.rsi_6,
    ind.cci,
    buy.rate_1,
    buy.rate_5
FROM cn_stock_indicators_buy buy
JOIN cn_stock_indicators ind 
    ON buy.code = ind.code AND buy.date = ind.date
WHERE buy.date = '2026-05-20';
```

---

## 十、回测验证

### 回测任务

回测任务由 `backtest_data_daily_job.py` 自动执行，计算选股后1-100日的收益率。

### 回测逻辑

```python
# 对于每个选股记录
for stock in selected_stocks:
    # 获取后续N日的收盘价
    future_prices = get_future_prices(stock.code, stock.date, N=100)
    
    # 计算收益率
    for i in range(1, 101):
        if i <= len(future_prices):
            rate_i = (future_prices[i] - stock.buy_price) / stock.buy_price * 100
        else:
            rate_i = None  # 没有足够的数据
    
    # 更新到数据库
    update_rates(stock.code, stock.date, rates)
```

### 查看回测结果

**示例1：查看某日选股的1日收益率分布**
```sql
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN rate_1 > 0 THEN 1 ELSE 0 END) as positive,
    SUM(CASE WHEN rate_1 <= 0 THEN 1 ELSE 0 END) as negative,
    AVG(rate_1) as avg_rate,
    MAX(rate_1) as max_rate,
    MIN(rate_1) as min_rate
FROM cn_stock_indicators_buy
WHERE date = '2026-05-20';
```

**示例2：查看某只股票的累计收益率**
```sql
SELECT 
    date,
    code,
    name,
    rate_1,
    rate_3,
    rate_5,
    rate_10,
    rate_20
FROM cn_stock_indicators_buy
WHERE code = '000001'
ORDER BY date DESC
LIMIT 10;
```

**示例3：统计最近30天的策略表现**
```sql
SELECT 
    date,
    COUNT(*) as stock_count,
    AVG(rate_1) as avg_rate_1,
    AVG(rate_5) as avg_rate_5,
    AVG(rate_10) as avg_rate_10
FROM cn_stock_indicators_buy
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC;
```

### 回测指标解读

| 指标 | 含义 | 优秀标准 |
|------|------|----------|
| **胜率** | 盈利次数/总次数 | > 60% |
| **平均收益率** | 所有交易的平均收益 | > 2% |
| **最大收益率** | 单笔最高收益 | > 10% |
| **最大回撤** | 单笔最大亏损 | < -5% |
| **盈亏比** | 平均盈利/平均亏损 | > 2:1 |

---

## 📝 总结

### 策略定位

`cn_stock_indicators_buy` 是一个**短线强势股追涨策略**，适合：
- ✅ 有一定经验的投资者
- ✅ 能够严格执行止损纪律
- ✅ 有充足时间盯盘
- ✅ 风险承受能力较强

### 核心价值

1. **自动化筛选**：从4000+只股票中快速找出强势股
2. **量化标准**：明确的选股条件，避免主观判断
3. **回测验证**：提供历史收益率数据，验证策略有效性
4. **持续优化**：可根据回测结果调整参数

### 风险提示

⚠️ **本策略仅供参考，不构成投资建议**

- 股市有风险，投资需谨慎
- 过往业绩不代表未来表现
- 请根据自身风险承受能力决策
- 建议先用小资金测试

---

## 🔗 相关文档

- [技术指标计算说明](./技术指标计算说明.md)
- [回测任务使用说明](./回测任务使用说明.md)
- [每日任务调度说明](./每日任务调度说明.md)

---

**文档版本**：v1.0  
**最后更新**：2026-05-21  
**维护者**：AI助手
