# 数据库字段迁移说明

## 📋 迁移概述

**迁移内容：** 将 `cn_stock_lhb` 表中的 `ranking_times` 字段重命名为 `ranking_date`

**迁移原因：**
- `ranking_times` 字面意思是"上榜次数"，但实际存储的是"上榜日期"（DATE类型）
- 与 `cn_stock_top` 表的 `ranking_times`（上榜次数，FLOAT类型）容易混淆
- 为保持语义清晰和命名规范，改为 `ranking_date`

---

## ⚠️ 重要提示

### **执行前必读：**

1. **备份数据库！**
   ```bash
   mysqldump -u root -p instockdb > backup_before_migration.sql
   ```

2. **确保没有其他任务正在访问 `cn_stock_lhb` 表**
   - 停止所有正在运行的 job 脚本
   - 确保 Web 服务没有正在查询龙虎榜数据

3. **此操作不可逆**
   - 一旦执行，无法自动回滚
   - 如果需要回滚，必须从备份恢复

---

## 🚀 执行步骤

### **方法一：使用迁移脚本（推荐）**

```bash
# 1. 激活虚拟环境（如果使用）
.venv\Scripts\activate

# 2. 执行迁移脚本
python instock/job/migrate_ranking_field.py

# 3. 按提示确认执行
是否继续执行迁移？(yes/no): yes
```

**脚本会自动完成：**
- ✅ 检查旧字段是否存在
- ✅ 检查新字段是否已存在
- ✅ 执行字段重命名
- ✅ 验证迁移结果
- ✅ 显示示例数据

---

### **方法二：手动执行 SQL**

如果迁移脚本无法运行，可以手动执行 SQL：

```sql
-- 连接到数据库
USE instockdb;

-- 执行字段重命名
ALTER TABLE `cn_stock_lhb` 
CHANGE COLUMN `ranking_times` `ranking_date` DATE NULL COMMENT '上榜日';

-- 验证结果
DESCRIBE cn_stock_lhb;
SELECT ranking_date FROM cn_stock_lhb LIMIT 5;
```

---

## ✅ 验证迁移

### **1. 检查字段是否存在**

```sql
-- 查看表结构
DESCRIBE cn_stock_lhb;

-- 应该看到 ranking_date 字段，而不是 ranking_times
```

### **2. 查询数据**

```sql
-- 查询最新5条记录
SELECT code, name, ranking_date FROM cn_stock_lhb ORDER BY ranking_date DESC LIMIT 5;

-- 应该能看到日期格式的数据，如：2026-05-03
```

### **3. 测试 Web 界面**

1. 重启 Web 服务（如果正在运行）
   ```bash
   # 停止服务
   # Ctrl+C
   
   # 启动服务
   python instock/web/web_service.py
   ```

2. 清除浏览器缓存
   - Chrome: `Ctrl + Shift + Delete`
   - 或硬刷新: `Ctrl + F5`

3. 访问龙虎榜页面
   - 检查"上榜日"列是否正常显示
   - 检查日期格式是否为 `YYYY-MM-DD`
   - 检查排序是否正常

---

## 🔧 故障排除

### **问题1：迁移脚本报错 "表不存在"**

**原因：** `cn_stock_lhb` 表尚未创建

**解决方案：**
```bash
# 先运行初始化脚本创建表
python instock/job/init_job.py

# 然后再执行迁移
python instock/job/migrate_ranking_field.py
```

---

### **问题2：迁移脚本报错 "字段不存在"**

**原因：** 可能已经执行过迁移，或者表结构不同

**解决方案：**
```sql
-- 检查当前表结构
DESCRIBE cn_stock_lhb;

-- 如果已经有 ranking_date 字段，无需迁移
-- 如果两个字段都不存在，需要重新建表
```

---

### **问题3：Web 界面显示 `/OADate(...)` 原始格式**

**原因：** 浏览器缓存了旧的 JavaScript 文件

**解决方案：**
1. 硬刷新浏览器：`Ctrl + F5`
2. 清除浏览器缓存
3. 或者在无痕模式下打开

---

### **问题4：迁移后数据丢失**

**原因：** 不太可能发生，但以防万一

**解决方案：**
```bash
# 从备份恢复
mysql -u root -p instockdb < backup_before_migration.sql
```

---

## 📝 相关文件清单

### **已修改的代码文件：**

1. ✅ `instock/core/tablestructure.py` - 数据库字段定义
2. ✅ `instock/core/stockfetch.py` - 数据抓取列名映射
3. ✅ `instock/web/templates/stock_web_aggrid.html` - 前端日期字段列表
4. ✅ `instock/web/static/js/ag-grid-column-widths.js` - 列宽配置
5. ✅ `instock/core/singleton_stock_web_module_data.py` - 排序规则

### **新增的文件：**

1. ✅ `instock/job/migrate_ranking_field.py` - 迁移脚本
2. ✅ `instock/job/MIGRATION_GUIDE.md` - 本说明文档

### **保持不变的内容：**

- ❌ `cn_stock_top` 表的 `ranking_times`（上榜次数，FLOAT类型）- **不修改**

---

## 🎯 预期效果

迁移完成后：

| 项目 | 迁移前 | 迁移后 |
|------|--------|--------|
| 字段名 | `ranking_times` | `ranking_date` |
| 字段类型 | DATE | DATE |
| 中文名称 | 上榜日 | 上榜日 |
| 前端显示 | `/OADate(46142.0)/` ❌ | `2026-05-03` ✅ |
| 语义清晰度 | 易混淆 | 清晰明确 |

---

## 📞 需要帮助？

如果遇到问题，请检查：

1. **日志文件：** `instock/log/stock_execute_job.log`
2. **浏览器控制台：** 按 `F12` 查看是否有 JavaScript 错误
3. **数据库日志：** MySQL 的错误日志

---

**祝您迁移顺利！** 😊
