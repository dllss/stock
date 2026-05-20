#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
数据库字段重命名迁移脚本
========================

功能说明：
将 cn_stock_lhb 表中的 ranking_times 字段重命名为 ranking_date

背景：
- ranking_times 原意为"上榜日"（DATE类型）
- 但字段名容易与 cn_stock_top 表的 ranking_times（上榜次数，FLOAT类型）混淆
- 为保持语义清晰和命名规范，改为 ranking_date

影响范围：
- 仅影响 cn_stock_lhb 表
- cn_stock_top 表的 ranking_times（上榜次数）保持不变

使用前必读：
1. 备份数据库！
2. 确保没有正在运行的任务访问 cn_stock_lhb 表
3. 执行后验证数据是否正确

使用方法：
    python instock/job/migrate_ranking_field.py

作者：AI Assistant
日期：2026/05/04
"""

import logging
import sys
import os

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.database as mdb
import pymysql

__author__ = 'AI Assistant'
__date__ = '2026/05/04'


def check_field_exists(table_name, field_name):
    """
    检查表中是否存在指定字段
    
    参数：
        table_name: 表名
        field_name: 字段名
        
    返回：
        bool: 字段是否存在
    """
    try:
        sql = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{mdb.db_database}' AND TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{field_name}'"
        result = mdb.executeSqlFetch(sql)
        
        if result and len(result) > 0:
            count = result[0][0]
            return count > 0
        
        return False
    except Exception as e:
        logging.error(f"检查字段失败: {e}")
        return False


def migrate_ranking_field():
    """
    执行字段重命名迁移
    
    步骤：
    1. 检查旧字段是否存在
    2. 检查新字段是否已存在
    3. 执行重命名
    4. 验证迁移结果
    """
    
    table_name = 'cn_stock_lhb'
    old_field = 'ranking_times'
    new_field = 'ranking_date'
    
    logging.info("=" * 80)
    logging.info("开始执行数据库字段迁移")
    logging.info("=" * 80)
    logging.info(f"表名: {table_name}")
    logging.info(f"旧字段: {old_field} (DATE)")
    logging.info(f"新字段: {new_field} (DATE)")
    logging.info("=" * 80)
    
    # 步骤1: 检查旧字段是否存在
    logging.info("\n步骤1: 检查旧字段是否存在...")
    if not check_field_exists(table_name, old_field):
        logging.warning(f"⚠️ 旧字段 '{old_field}' 不存在，可能已经迁移过或表不存在")
        
        # 检查新字段是否存在
        if check_field_exists(table_name, new_field):
            logging.info(f"✅ 新字段 '{new_field}' 已存在，无需迁移")
            return True
        else:
            logging.error(f"❌ 表 '{table_name}' 可能不存在，请先运行 init_job.py 创建表")
            return False
    
    logging.info(f"✅ 旧字段 '{old_field}' 存在")
    
    # 步骤2: 检查新字段是否已存在
    logging.info("\n步骤2: 检查新字段是否已存在...")
    if check_field_exists(table_name, new_field):
        logging.warning(f"⚠️ 新字段 '{new_field}' 已存在")
        logging.warning("可能的原因：")
        logging.warning("  1. 已经执行过迁移")
        logging.warning("  2. 手动创建了新字段")
        logging.warning("\n建议：")
        logging.warning("  - 如果确认已迁移，可以跳过")
        logging.warning("  - 如果需要重新迁移，请先删除新字段")
        
        response = input("\n是否继续？(y/n): ")
        if response.lower() != 'y':
            logging.info("用户取消迁移")
            return False
    
    # 步骤3: 执行重命名
    logging.info("\n步骤3: 执行字段重命名...")
    
    try:
        # 使用 ALTER TABLE CHANGE COLUMN 重命名字段
        # CHANGE COLUMN 可以同时修改字段名和字段定义
        alter_sql = f"""
            ALTER TABLE `{table_name}` 
            CHANGE COLUMN `{old_field}` `{new_field}` DATE NULL COMMENT '上榜日'
        """
        
        logging.info(f"执行SQL: {alter_sql.strip()}")
        
        # 执行SQL
        mdb.executeSql(alter_sql)
        
        logging.info(f"✅ 字段重命名成功: {old_field} → {new_field}")
        
    except Exception as e:
        logging.error(f"❌ 字段重命名失败: {e}")
        logging.error("\n可能的原因：")
        logging.error("  1. 表不存在")
        logging.error("  2. 字段不存在")
        logging.error("  3. 数据库权限不足")
        logging.error("  4. 有其他事务锁定了表")
        return False
    
    # 步骤4: 验证迁移结果
    logging.info("\n步骤4: 验证迁移结果...")
    
    # 检查旧字段是否还存在
    if check_field_exists(table_name, old_field):
        logging.error(f"❌ 验证失败：旧字段 '{old_field}' 仍然存在")
        return False
    
    logging.info(f"✅ 旧字段 '{old_field}' 已删除")
    
    # 检查新字段是否存在
    if not check_field_exists(table_name, new_field):
        logging.error(f"❌ 验证失败：新字段 '{new_field}' 不存在")
        return False
    
    logging.info(f"✅ 新字段 '{new_field}' 已创建")
    
    # 查询一条数据验证
    try:
        verify_sql = f"SELECT `{new_field}` FROM `{table_name}` LIMIT 1"
        result = mdb.executeSqlFetch(verify_sql)
        
        if result is not None:
            logging.info(f"✅ 数据验证通过，可以正常查询新字段")
            if len(result) > 0 and result[0][0] is not None:
                logging.info(f"   示例数据: {result[0][0]}")
        else:
            logging.info(f"ℹ️ 表为空，无法验证数据（这是正常的）")
            
    except Exception as e:
        logging.warning(f"⚠️ 数据验证失败: {e}")
        logging.warning("   但这不影响迁移结果，可能是表为空")
    
    logging.info("\n" + "=" * 80)
    logging.info("✅ 迁移完成！")
    logging.info("=" * 80)
    logging.info("\n后续操作：")
    logging.info("  1. 重启 Web 服务（如果正在运行）")
    logging.info("  2. 清除浏览器缓存")
    logging.info("  3. 访问龙虎榜页面验证数据显示")
    logging.info("=" * 80)
    
    return True


def main():
    """主函数"""
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logging.info("\n" + "=" * 80)
    logging.info("数据库字段迁移工具")
    logging.info("=" * 80)
    logging.info("\n⚠️ 重要提示：")
    logging.info("  1. 此操作会修改数据库结构")
    logging.info("  2. 建议先备份数据库")
    logging.info("  3. 确保没有其他任务正在访问 cn_stock_lhb 表")
    logging.info("=" * 80)
    
    # 确认执行
    response = input("\n是否继续执行迁移？(yes/no): ")
    
    if response.lower() not in ['yes', 'y']:
        logging.info("用户取消操作")
        sys.exit(0)
    
    # 执行迁移
    success = migrate_ranking_field()
    
    if success:
        logging.info("\n✅ 迁移成功完成！")
        sys.exit(0)
    else:
        logging.error("\n❌ 迁移失败！")
        sys.exit(1)


if __name__ == '__main__':
    main()
