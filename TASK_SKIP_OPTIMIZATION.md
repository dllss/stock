# 任务数据检查优化 - 修改指南

## 🎯 优化目标

**问题**: 当前任务每次执行都会删除当天的旧数据并重新从服务器抓取,即使数据已经存在。

**解决方案**: 在执行任务前先检查数据库中是否已有当天的数据,如果有则跳过,避免不必要的网络请求。

---

## ✅ 已完成的修改

### **工具函数**
- ✅ [task_utils.py](file://d:/WorkProject/stock/instock/job/task_utils.py) - 通用检查函数

### **已修改的任务文件** (3个)
1. ✅ [cn_stock_lhb_job.py](file://d:/WorkProject/stock/instock/job/data_tasks/cn_stock_lhb_job.py) - 龙虎榜明细
2. ✅ [cn_stock_top_job.py](file://d:/WorkProject/stock/instock/job/data_tasks/cn_stock_top_job.py) - 龙虎榜汇总  
3. ✅ [cn_stock_bonus_job.py](file://d:/WorkProject/stock/instock/job/data_tasks/cn_stock_bonus_job.py) - 分红配送

---

## 📋 待修改的文件列表 (8个)

4. ⏳ cn_stock_blocktrade_job.py - 大宗交易
5. ⏳ cn_stock_chip_race_end_job.py - 尾盘抢筹
6. ⏳ cn_stock_chip_race_open_job.py - 早盘抢筹
7. ⏳ cn_stock_fund_flow_concept_job.py - 概念资金流向
8. ⏳ cn_stock_fund_flow_industry_job.py - 行业资金流向
9. ⏳ cn_stock_fund_flow_job.py - 个股资金流向
10. ⏳ cn_stock_limitup_reason_job.py - 涨停原因
11. ⏳ cn_stock_selection_job.py - 综合选股
12. ⏳ cn_stock_spot_job.py - 股票基础数据
13. ⏳ cn_etf_spot_job.py - ETF基础数据

---

## 🔧 修改模式

每个任务文件需要添加两处修改:

### **修改1: 添加工具函数导入**

在文件顶部的import区域,找到:
```python
import instock.core.stockfetch as stf
```

在其后添加:
```python
from instock.job.task_utils import check_and_skip_if_exists
```

### **修改2: 添加数据检查逻辑**

在任务的 `try:` 块开始处,将原来的代码:

```python
try:
    logging.info("")
    logging.info("=" * 20)
    logging.info(f"[{date}] 开始获取XXX数据...")
    
    data = stf.fetch_xxx_data(date)
    if data is None or len(data.index) == 0:
        logging.info("XXX数据为空，跳过")
        return

    table_name = tbs.TABLE_XXX['name']
```

改为:

```python
try:
    table_name = tbs.TABLE_XXX['name']
    
    # 步骤1: 检查当天是否已有数据
    if check_and_skip_if_exists(table_name, date):
        return
    
    # 步骤2: 抓取数据
    logging.info("")
    logging.info("=" * 20)
    logging.info(f"[{date}] 开始获取XXX数据...")
    
    data = stf.fetch_xxx_data(date)
    if data is None or len(data.index) == 0:
        logging.info("XXX数据为空，跳过")
        return

    # 步骤3: 准备插入数据
```

---

## 💡 核心逻辑

### **check_and_skip_if_exists() 函数工作流程:**

```python
def check_and_skip_if_exists(table_name, date):
    """
    1. 检查表是否存在
    2. 如果不存在 → 返回 False (需要抓取)
    3. 如果存在 → 查询当天数据量
    4. 如果 count > 0 → 返回 True (跳过)
    5. 如果 count = 0 → 返回 False (需要抓取)
    """
```

### **执行流程对比:**

#### **修改前:**
```
任务启动
  ↓
抓取数据 (网络请求)
  ↓
删除当天旧数据 (DELETE)
  ↓
插入新数据 (INSERT)
  ↓
完成
```

#### **修改后:**
```
任务启动
  ↓
检查当天是否有数据 (SELECT COUNT)
  ↓
有数据? ──→ 是 ──→ 跳过,直接结束 ✅
  ↓
 否
  ↓
抓取数据 (网络请求)
  ↓
删除当天旧数据 (防御性编程)
  ↓
插入新数据 (INSERT)
  ↓
完成
```

---

## 📊 优势

### **1. 减少不必要的网络请求**
- ✅ 如果数据已存在,完全跳过抓取
- ✅ 节省API调用次数
- ✅ 降低被反爬的风险

### **2. 提高执行效率**
- ✅ SELECT COUNT 比完整抓取快得多
- ✅ 避免重复数据处理
- ✅ 减少数据库写入操作

### **3. 更好的日志输出**
```
✅ cn_stock_lhb 表在 2026-05-12 已有 497 条数据，跳过抓取
```
清晰明了,便于排查问题

### **4. 防御性编程**
- ✅ 即使有数据,仍然保留DELETE逻辑(理论上不会触发)
- ✅ 保证数据一致性
- ✅ 应对异常情况

---

## ⚠️ 注意事项

### **1. 性能考虑**
- SELECT COUNT 非常快 (< 0.01秒)
- 相比完整的网络抓取(10-60秒),开销可以忽略
- 总体性能提升显著

### **2. 数据一致性**
- 使用 `date` 字段作为唯一标识
- 确保同一天只有一份数据
- DELETE + INSERT 保证原子性

### **3. 特殊情况**
- 如果需要强制刷新数据,可以手动DELETE后再运行
- 或者临时注释掉检查逻辑

---

## 🚀 如何继续修改

### **选项1: 我继续批量修改**
我可以继续逐个修改剩余的8个文件。

### **选项2: 你手动修改**
按照上面的修改模式,每个文件只需:
1. 添加1行import
2. 移动table_name定义到try开头
3. 添加3行检查代码

### **选项3: 使用脚本**
运行 `scripts/maintenance/batch_modify_tasks.py` 自动添加import,然后手动添加检查逻辑。

---

## 📝 测试验证

修改完成后,运行任务验证:

```bash
# 第一次运行 - 应该正常抓取
python instock/job/data_tasks/cn_stock_lhb_job.py

# 第二次运行 - 应该跳过
python instock/job/data_tasks/cn_stock_lhb_job.py
```

预期输出:
```
✅ cn_stock_lhb 表在 2026-05-12 已有 497 条数据，跳过抓取
```

---

## 📚 相关文件

- [task_utils.py](file://d:/WorkProject/stock/instock/job/task_utils.py) - 工具函数
- [MIGRATION_COMPLETE.md](file://d:/WorkProject/stock/MIGRATION_COMPLETE.md) - 延迟配置迁移报告

---

## ✅ 总结

这个优化非常重要,可以:
- ✅ 避免重复抓取
- ✅ 节省时间和资源
- ✅ 降低反爬风险
- ✅ 提高系统稳定性

**建议尽快完成所有文件的修改!** 🚀
