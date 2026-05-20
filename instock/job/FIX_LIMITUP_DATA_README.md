# 涨停原因数据修复脚本使用说明

## 📋 功能说明

本脚本用于补充 `cn_stock_limitup_reason` 表中缺失的行情数据。

**问题背景**：
- 同花顺API对某些历史日期不提供完整的行情数据（最新价、涨跌幅等）
- 导致涨停原因表中有大量空值，影响数据分析

**解决方案**：
- 从 `cn_stock_spot` 表中获取对应日期的行情数据
- 通过股票代码匹配，补充缺失的字段

---

## 🚀 使用方法

### 1. 检查数据（预览模式）

```bash
python -m instock.job.fix_cn_stock_limitup_reason_data --date 2026-05-15
```

输出示例：
```
============================================================
检查 2026-05-15 涨停原因数据
============================================================
总记录数: 72
最新价为空: 70
有数据的记录: 2

⚠️  发现缺失数据，需要补充

从 cn_stock_spot 表获取 2026-05-15 涨停股票的行情数据...
找到 72 只需要更新的涨停股票
✅ 获取到 70 条行情数据

生成UPDATE SQL...
✅ 生成 70 条UPDATE语句

============================================================
预览前 3 条UPDATE语句:
============================================================
...
```

### 2. 导出SQL文件（推荐）

```bash
python -m instock.job.fix_cn_stock_limitup_reason_data --date 2026-05-15 --export
```

会在 `instock/job/` 目录下生成：
- `update_limitup_20260515.sql`

然后在数据库工具中执行该SQL文件。

### 3. 直接执行更新

```bash
python -m instock.job.fix_cn_stock_limitup_reason_data --date 2026-05-15 --execute
```

会要求确认后才执行，显示进度：
```
⚠️  即将执行数据库更新操作，请确认...
确认执行？(yes/no): yes

============================================================
开始执行UPDATE...
============================================================
进度: 10/70 (成功:10, 失败:0)
进度: 20/70 (成功:20, 失败:0)
...
执行完成！成功:70, 失败:0

✅ 所有更新执行成功！
```

---

## ⚙️ 参数说明

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `--date` | string | ✅ | 目标日期，格式：YYYY-MM-DD |
| `--execute` | flag | ❌ | 直接执行UPDATE（需确认） |
| `--export` | flag | ❌ | 导出SQL文件 |

---

## 💡 使用建议

### 推荐流程

1. **先检查** - 确认有多少数据需要补充
   ```bash
   python -m instock.job.fix_cn_stock_limitup_reason_data --date 2026-05-15
   ```

2. **导出SQL** - 在数据库工具中审查
   ```bash
   python -m instock.job.fix_cn_stock_limitup_reason_data --date 2026-05-15 --export
   ```

3. **手动执行** - 在数据库工具中运行SQL文件

### 注意事项

- ⚠️ 确保 `cn_stock_spot` 表中已有目标日期的基础数据
- ⚠️ 只更新 `new_price IS NULL OR new_price = 0` 的记录，不会覆盖已有数据
- ⚠️ 如果某些股票在 `cn_stock_spot` 中也没有数据，将无法补充（可能停牌或退市）

---

## 🔍 常见问题

### Q1: 为什么有些股票还是空值？

**A**: 这些股票在 `cn_stock_spot` 表中也没有数据，可能是：
- 当天停牌
- 科创板股票（688开头）数据未抓取
- 新股上市初期数据不完整

### Q2: 可以批量修复多个日期吗？

**A**: 目前不支持，需要逐个日期执行。可以编写批处理脚本：

```bash
# Windows PowerShell
$dates = @("2026-05-14", "2026-05-15", "2026-05-16")
foreach ($date in $dates) {
    python -m instock.job.fix_cn_stock_limitup_reason_data --date $date --export
}
```

### Q3: 执行失败怎么办？

**A**: 
1. 检查数据库连接是否正常
2. 确认 `cn_stock_spot` 表中有对应日期的数据
3. 查看错误日志，定位具体问题

---

## 📝 技术细节

### SQL逻辑

```sql
UPDATE cn_stock_limitup_reason 
SET new_price={new_price}, 
    change_rate={change_rate}, 
    ups_downs={ups_downs}, 
    volume={volume}, 
    deal_amount={deal_amount}, 
    turnoverrate={turnoverrate}
WHERE date='2026-05-15' 
  AND code='{code}' 
  AND (new_price IS NULL OR new_price = 0);
```

### 安全机制

- ✅ 只更新空值记录，不覆盖已有数据
- ✅ 使用事务包装（BEGIN/COMMIT）
- ✅ 逐条执行，记录失败情况
- ✅ 执行前需要用户确认

---

## 📂 相关文件

- 脚本位置：`instock/job/fix_cn_stock_limitup_reason_data.py`
- 数据表：`cn_stock_limitup_reason`（涨停原因）
- 数据源：`cn_stock_spot`（基础行情）
- 相关任务：`instock/job/data_tasks/cn_stock_limitup_reason_job.py`

---

## ✨ 更新日志

- 2026-05-04: 初始版本，支持单日期修复
- 支持导出SQL和直接执行两种模式
- 自动从涨停股票列表筛选，避免全量更新
