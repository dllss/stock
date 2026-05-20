# InStock/bin 用户脚本指南

## 📁 **说明**

本文件夹包含 **用户日常使用的便捷脚本**，用于简化股票数据的更新和分析任务。

---

## 📋 **可用脚本**

### **1. quick_update.bat** - 快速更新

**功能：** 仅更新基础数据（实时行情、资金流向等）  
**耗时：** 15-20分钟  
**适用场景：** 每个交易日收盘后

**使用方法：**
```bash
# 双击运行
instock\bin\quick_update.bat

# 或在命令行中运行
cd d:\WorkProject\stock
instock\bin\quick_update.bat
```

---

### **2. technical_analysis.bat** - 技术分析 ⭐ **新增**

**功能：** 仅运行技术分析和选股任务（**不包含**基础数据抓取）  
**耗时：** 30-50分钟  
**适用场景：** 已有基础数据，只需要计算指标和选股

**前置条件：**
- ✅ 已运行 `execute_daily_job.py`（基础数据）
- ✅ 已运行 `historical_data_job.py`（历史K线数据）

**执行步骤：**
1. 技术指标计算
2. K线形态识别
3. 策略选股

**使用方法：**
```bash
# 双击运行
instock\bin\technical_analysis.bat

# 或在命令行中运行
cd d:\WorkProject\stock
instock\bin\technical_analysis.bat
```

---

### **3. full_analysis.bat** - 完整分析

**功能：** 依次运行所有数据分析任务（**包含**基础数据抓取）  
**耗时：** 60-90分钟  
**适用场景：** 周末或节假日进行全面分析

**执行步骤：**
1. **基础数据更新** ← 包含此步骤
2. 技术指标计算
3. K线形态识别
4. 策略选股

**使用方法：**
```bash
# 双击运行
instock\bin\full_analysis.bat

# 或在命令行中运行
cd d:\WorkProject\stock
instock\bin\full_analysis.bat
```

---

### **4. run_job.bat** - 系统作业（原有）

**功能：** 执行每日作业任务  
**适用场景：** 系统级任务执行

---

### **5. run_web.bat** - 启动Web服务（原有）

**功能：** 启动InStock Web服务  
**适用场景：** 启动Web界面

---

##  **使用建议**

### **工作日（周一至周五）**

```bash
# 收盘后（15:30-17:00）运行快速更新
instock\bin\quick_update.bat
```

**产出：**
- ✅ 最新行情数据
- ✅ 资金流向
- ✅ 龙虎榜数据
- ❌ 技术指标（可选）
- ❌ 选股结果（可选）

---

### **周末/节假日 - 方案A：已有基础数据**

```bash
# 如果已经运行过 quick_update.bat，只需技术分析
instock\bin\technical_analysis.bat
```

**耗时：** 30-50分钟  
**产出：**
- ✅ 技术指标
- ✅ K线形态
- ✅ 选股结果

---

### **周末/节假日 - 方案B：从头开始**

```bash
# 如果需要从基础数据开始
instock\bin\full_analysis.bat
```

**耗时：** 60-90分钟  
**产出：**
- ✅ 全部数据
- ✅ 技术指标
- ✅ K线形态
- ✅ 选股结果

---

## 📊 **脚本对比**

| 脚本 | 基础数据 | 技术指标 | K线形态 | 策略选股 | 总耗时 |
|------|---------|---------|---------|---------|--------|
| **quick_update.bat** | ✅ | ❌ | ❌ | ❌ | 15-20分钟 |
| **technical_analysis.bat** | ❌ | ✅ | ✅ | ✅ | 30-50分钟 |
| **full_analysis.bat** | ✅ | ✅ | ✅ | ✅ | 60-90分钟 |

---

## 🎯 **如何选择？**

### **场景1：工作日收盘后**
```
✅ 使用：quick_update.bat
原因：只需要最新的基础数据，快速完成
```

### **场景2：周末，已有基础数据**
```
✅ 使用：technical_analysis.bat
原因：基础数据已经有了，只需技术分析
```

### **场景3：周末，从头开始**
```
✅ 使用：full_analysis.bat
原因：需要完整的数据更新和分析
```

### **场景4：只想要某个特定任务**
```
✅ 单独运行对应的Python脚本：
python instock/job/indicators_data_daily_job.py      # 只计算指标
python instock/job/klinepattern_data_daily_job.py    # 只识别形态
python instock/job/strategy_data_daily_job.py        # 只选股
```

---

## ⚠️ **注意事项**

1. **确保Python环境已激活**
   ```bash
   # 如果使用虚拟环境
   .venv\Scripts\activate
   ```

2. **检查网络连接**
   - 需要访问东方财富网API
   - 确保网络稳定

3. **查看日志**
   - 如果脚本报错，请查看：`instock\log\stock_execute_job.log`
   - 日志中包含详细的错误信息

4. **耐心等待**
   - 完整分析可能需要60-90分钟
   - 不要在运行过程中关闭窗口

---

## 📝 **相关文档**

详细的使用指南请参考：
- [独立任务运行指南](../../docs/INDEPENDENT_TASKS_GUIDE.md)

---

**祝您使用愉快！** 😊
