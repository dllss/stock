# Scripts 文件夹

## 📁 **说明**

本文件夹包含 **开发和调试工具脚本**,用于项目维护和测试。

---

## ⚠️ **重要提示**

**用户使用的便捷脚本已移至：** `instock/bin/`

请前往 `instock/bin/` 目录使用以下脚本：
- `quick_update.bat` - 快速更新基础数据
- `full_analysis.bat` - 完整数据分析  
- `technical_analysis.bat` - 技术分析（不含基础数据）
- `run_data_tasks.bat` - 批量执行数据任务
- `run_chip_race_end.bat` - 运行尾盘抢筹任务

详见：[instock/bin/README_USER_GUIDE.md](../instock/bin/README_USER_GUIDE.md)

---

## 📋 **本目录内容**

### 🔧 **核心工具脚本** (8个)

| 脚本名称 | 功能说明 | 使用场景 |
|---------|---------|----------|
| **generate_aggrid_config.py** | AG Grid列宽配置生成器 | 分析数据库表结构,自动生成前端列宽配置 |
| **query_daily_close.py** | 股票收盘数据查询工具 | 查询特定日期的股票数据,支持筛选和排序 |
| **check_database_integrity.py** | 数据库完整性检查工具 | 检查ETF数据、异常值、表结构等 |
| **test_fund_flow_comprehensive.py** | 资金流向API综合测试 | 测试资金流向API连接、数据获取、保存等功能 |
| **switch_browser.py** | 浏览器Cookie切换工具 | 在多个浏览器间切换获取Cookie |
| **test_proxy.py** | 代理服务器测试工具 | 测试代理IP的可用性和速度 |
| **test_proxy_ip.py** | 代理IP验证工具 | 验证代理IP的真实性和地理位置 |
| **add_units_to_headers.py** | 表头单位批量添加工具 | 为tablestructure.py中的字段添加单位(%) |

### 📈 **除权排查脚本**

| 脚本名称 | 功能说明 | 使用场景 |
|---------|---------|----------|
| **adjustment/analyze_adjustment.py** | 唯特偶除权价格差异分析 | 手工验证除权前后价格和涨跌幅 |
| **adjustment/check_bonus_data.py** | 查询最近除权记录 | 检查 cn_stock_bonus 最近除权数据 |
| **adjustment/check_price_jumps.py** | 检查价格异常跳变 | 定位 cn_stock_spot 中疑似除权跳变 |
| **adjustment/check_weiteou.py** | 查询唯特偶收益率 | 排查 301319 回测收益率数据 |
| **adjustment/check_weiteou_price.py** | 查询唯特偶价格 | 排查 301319 cn_stock_spot 价格 |
| **adjustment/fix_adjustment_data.py** | 除权修复方案草稿 | 生成修复计划，默认模拟执行 |
| **adjustment/fix_adjustment_data_complete.py** | 除权修复脚本完整版 | 手工执行历史数据修复 |
| **adjustment/query_ex_dividend_stocks.py** | 查询指定日期除权股票 | 排查某天除权股票列表 |

### 🧰 **维护和演示脚本**

| 脚本名称 | 功能说明 | 使用场景 |
|---------|---------|----------|
| **maintenance/batch_modify_tasks.py** | 批量添加任务数据检查 import | 历史任务改造辅助 |
| **maintenance/quick_add_imports.py** | 快速批量添加 import | 历史任务改造辅助 |
| **demo/demo_realtime_config.py** | 动态延迟配置演示 | 验证 delay_config.json 实时生效 |

---

## 🚀 **快速开始**

以下命令默认在项目根目录执行。

### 1. AG Grid配置生成
```bash
python scripts/generate_aggrid_config.py
```
生成所有表的AG Grid列宽配置,输出到控制台。

### 2. 查询股票数据
```bash
# 查询某日所有股票
python scripts/query_daily_close.py 2024-01-15

# 查询涨跌幅前20
python scripts/query_daily_close.py 2024-01-15 --top 20 --order-by change_rate
```

### 3. 数据库完整性检查
```bash
# 检查所有表
python scripts/check_database_integrity.py

# 仅检查ETF数据
python scripts/check_database_integrity.py --check etf
```

### 4. 资金流向API测试
```bash
# 运行所有测试
python scripts/test_fund_flow_comprehensive.py

# 仅测试API连接
python scripts/test_fund_flow_comprehensive.py --test api
```

### 5. 除权排查
```bash
python scripts/adjustment/check_bonus_data.py
python scripts/adjustment/check_price_jumps.py
python scripts/adjustment/query_ex_dividend_stocks.py
```

### 6. 动态配置演示
```bash
python scripts/demo/demo_realtime_config.py
```

---

## 📝 **开发说明**

### 脚本分类

**工具类脚本** (长期保留):
- `generate_aggrid_config.py` - 配置生成器
- `query_daily_close.py` - 数据查询工具
- `check_database_integrity.py` - 数据库检查
- `add_units_to_headers.py` - 表头单位工具

**测试类脚本** (根据需要保留):
- `test_fund_flow_comprehensive.py` - 资金流向测试
- `test_proxy.py` / `test_proxy_ip.py` - 代理测试
- `switch_browser.py` - 浏览器切换

### 维护建议

1. **定期清理**: 删除临时测试文件和日志
2. **合并重复**: 将功能相似的测试脚本合并为综合测试
3. **文档更新**: 新增脚本时同步更新本README
4. **移动用户脚本**: 面向用户的便捷脚本应移至 `instock/bin/`

---

## 📅 **整理记录**

| 日期 | 操作 | 说明 |
|------|------|------|
| 2026-05-10 | 删除重复测试脚本 | 删除4个资金流向测试脚本,合并为comprehensive版本 |
| 2026-05-10 | 移动批处理文件 | run_data_tasks.bat 和 run_chip_race_end.bat 移至 instock/bin/ |
| 2026-05-10 | 删除临时文件 | test_demo.db, test_date_param.html |
| 2026-05-10 | 更新README | 反映最新的脚本结构和用途 |
| 2026-05-24 | 整理根目录脚本 | 除权排查、维护、演示脚本移入 scripts 子目录 |

---

**开发愉快！** 😊
