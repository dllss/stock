#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
数据库初始化模块（系统初始化）
==============================
这是系统的第一个任务，负责创建数据库和基础表。

主要功能：
1. 检查数据库是否存在
2. 如果不存在，创建数据库
3. 创建基础表（关注表）

什么时候运行？
- 系统第一次使用时
- 数据库被删除后
- 每次execute_daily_job都会运行（自动检查）

为什么每次都运行？
- 自动检查数据库存在性
- 如果存在，跳过（不影响性能）
- 如果不存在，自动创建
- 确保系统正常运行

创建的内容：
1. 数据库：instockdb
2. 基础表：cn_stock_attention（我的关注表）
3. 其他表：由各个job自动创建

设计思想：
- 自动化：无需手动创建
- 安全性：检查后再创建
- 幂等性：多次运行结果一样
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import pymysql  # MySQL数据库驱动
import os.path  # 路径操作
import sys  # 系统操作

# ==================== 路径和日志配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

from instock.lib.logger_config import setup_job_logging
setup_job_logging()

# ==================== 导入项目模块 ====================
import instock.lib.database as mdb  # 数据库配置

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 创建数据库 ====================

"""
创建新数据库
功能说明：
1. 连接到MySQL服务器（不指定数据库）
2. 创建instockdb数据库（如果不存在）
3. 设置字符集为utf8mb4（支持emoji等）
4. 创建基础表
执行流程：
1. 复制数据库配置
2. 修改为连接mysql系统库
3. 执行CREATE DATABASE语句
4. 调用创建基础表函数
SQL语句：
CREATE DATABASE IF NOT EXISTS `instockdb`
CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
字符集说明：
- utf8mb4：UTF-8的完整实现
- 支持所有Unicode字符（包括emoji）
- utf8mb4_general_ci：不区分大小写的排序规则
IF NOT EXISTS：
- 如果数据库已存在，不会报错
- 如果数据库不存在，创建它
- 幂等性：多次运行结果一样
"""
def create_new_database():
    # ==================== 步骤1: 准备连接配置 ====================
    # 复制原配置
    _MYSQL_CONN_DBAPI = mdb.MYSQL_CONN_DBAPI.copy()
    # 修改database为mysql（MySQL的系统数据库）
    # 因为我们要创建数据库，不能连接到还不存在的instockdb
    _MYSQL_CONN_DBAPI['database'] = "mysql"
    
    # ==================== 步骤2: 连接MySQL并创建数据库 ====================
    # with语句：自动管理连接和游标的关闭
    try:
        with pymysql.connect(**_MYSQL_CONN_DBAPI) as conn:
            with conn.cursor() as db:
                try:
                    # 构建CREATE DATABASE语句
                    # IF NOT EXISTS：如果不存在才创建
                    # CHARACTER SET：字符集
                    # COLLATE：排序规则
                    create_sql = f"CREATE DATABASE IF NOT EXISTS `{mdb.db_database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
                    # 执行SQL语句
                    db.execute(create_sql)
                    
                    logging.info(f"数据库创建成功：{mdb.db_database}")
                    
                    # 数据库创建成功，创建基础表
                    create_new_base_table()
                    
                except Exception as e:
                    # 记录错误日志
                    logging.error(f"[ERROR][init_job][create_new_database][db.execute]: {e}")
    except Exception as e:
        logging.error(f"[ERROR][init_job][create_new_database][pymysql.connect]: {e}")
        raise e


# ==================== 创建基础表 ====================

"""
创建基础表：我的关注表
功能说明：
创建cn_stock_attention表
用于存储用户关注的股票
表结构：
字段：
- datetime：关注时间（可为空）
- code：股票代码（主键，不能为空）
主键：
- code：股票代码
- 确保一只股票只关注一次
- 不能重复关注
索引：
- INIX_DATETIME：按时间索引
- 加速按时间查询
- USING BTREE：使用B树索引
表选项：
- CHARACTER SET：utf8mb4
- COLLATE：utf8mb4_general_ci
- ROW_FORMAT：Dynamic（动态行格式）
为什么单独创建这个表？
- 关注功能是基础功能
- 其他表由各job自动创建
- 这个表需要用户操作（添加关注）
表用途：
- 存储用户关注的股票列表
- Web界面中可以添加/删除关注
- 关注的股票会在各个模块置顶、标红显示
使用示例：
# 添加关注
INSERT INTO cn_stock_attention (datetime, code) 
VALUES ('2024-01-01 10:00:00', '600000')
# 查询关注
SELECT * FROM cn_stock_attention
# 删除关注
DELETE FROM cn_stock_attention WHERE code = '600000'
"""
def create_new_base_table():
    # 连接到instockdb数据库（已创建）
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            # ==================== CREATE TABLE语句 ====================
            # IF NOT EXISTS：如果表不存在才创建
            # datetime(0)：datetime类型，精度0（秒）
            # NULL：可以为空
            # varchar(6)：可变长度字符串，最大6个字符
            # NOT NULL：不能为空
            # PRIMARY KEY：主键，使用BTREE索引
            # INDEX：普通索引
            create_table_sql = """CREATE TABLE IF NOT EXISTS `cn_stock_attention` (
                                  `datetime` datetime(0) NULL DEFAULT NULL, 
                                  `code` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
                                  PRIMARY KEY (`code`) USING BTREE,
                                  INDEX `INIX_DATETIME`(`datetime`) USING BTREE
                                  ) CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
            
            # 执行创建表语句
            db.execute(create_table_sql)
            
            logging.info("基础表创建成功：cn_stock_attention")


# ==================== 检查数据库 ====================

"""
检查数据库是否存在且可连接
功能说明：
尝试连接数据库并执行简单查询
如果成功，说明数据库存在
如果失败，说明数据库不存在或无法连接
检查方法：
执行 "SELECT 1"
- 最简单的SQL查询
- 只是测试连接
- 不返回任何有用数据
- 不访问任何表
为什么这样检查？
- 简单快速
- 不影响数据
- 只测试连接性
- 标准做法
异常：
如果数据库不存在，pymysql.connect()会抛出异常
异常被main()捕获，然后创建数据库
"""
def check_database():
    # 连接数据库
    with pymysql.connect(**mdb.MYSQL_CONN_DBAPI) as conn:
        with conn.cursor() as db:
            # 执行测试查询
            # SELECT 1：最简单的查询，只返回1
            db.execute(" select 1 ")
            # 如果执行成功，说明数据库存在且可连接


# ==================== 主函数 ====================

"""
初始化任务主函数
功能说明：
1. 检查数据库是否存在
2. 如果不存在，创建数据库
3. 如果存在，什么也不做
执行逻辑：
try:
check_database()  # 尝试连接数据库
# 成功：数据库存在，结束
except:
# 失败：数据库不存在
create_new_database()  # 创建数据库
为什么这样设计？
- 自动化：无需手动创建数据库
- 安全性：先检查再创建
- 幂等性：多次运行不会重复创建
- 容错性：自动处理数据库不存在的情况
使用场景：
1. 首次安装系统：自动创建数据库
2. 数据库被误删：自动恢复
3. 迁移到新服务器：自动初始化
4. 每日任务：检查数据库完整性
注意事项：
- MySQL服务必须已启动
- 用户必须有CREATE DATABASE权限
- 网络必须连通（如果是远程数据库）
- 配置必须正确（database.py中的配置）
"""
def main():
    # ==================== 检查数据库是否存在 ====================
    try:
        # 尝试连接并查询数据库
        check_database()
        
        # 如果执行到这里，说明数据库存在
        logging.info("数据库检查通过：数据库已存在")
        
    except Exception as e:
        # 捕获异常：数据库不存在或连接失败
        logging.error("执行信息：数据库不存在，将创建。")
        
        # 创建新数据库
        create_new_database()
        
        logging.info("数据库创建完成")
    
    # 任务完成
    logging.info("初始化任务完成")


# ==================== 程序入口 ====================
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    运行方式：
        python init_job.py
        
    执行结果：
        - 数据库不存在：创建数据库和基础表
        - 数据库存在：什么也不做，直接结束
        
    日志输出：
        - 终端：直接显示
        - 文件：instock/log/stock_execute_job.log
    """
    main()


"""
===========================================
初始化任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 系统初始化模块
   - 第一个执行的任务
   - 创建数据库和基础表

2. 核心功能
   - 检查数据库存在性
   - 创建数据库
   - 创建基础表

3. 执行时机
   - 系统首次运行
   - 每次execute_daily_job开始时
   - 数据库丢失后

4. 创建的内容
   数据库：
   - 名称：instockdb
   - 字符集：utf8mb4
   - 排序：utf8mb4_general_ci
   
   基础表：
   - cn_stock_attention：关注表
   - 主键：code（股票代码）
   - 索引：datetime（关注时间）

5. 幂等性
   定义：
   - 多次执行结果相同
   - 不会重复创建
   - 安全可靠
   
   实现：
   - IF NOT EXISTS：SQL语句
   - 如果存在，跳过
   - 如果不存在，创建

6. Python知识点
   - 异常处理：try-except
   - 字典操作：dict.copy()
   - with语句：资源管理
   - f-string：格式化字符串
   - 模块导入：import ... as ...

7. SQL知识点
   - CREATE DATABASE：创建数据库
   - CREATE TABLE：创建表
   - IF NOT EXISTS：条件创建
   - PRIMARY KEY：主键
   - INDEX：索引
   - CHARACTER SET：字符集

8. 使用说明
   手动运行：
   ```bash
   python instock/job/init_job.py
   ```
   
   自动运行：
   - execute_daily_job会自动调用
   - 无需手动运行

9. 故障排查
   问题1：无法创建数据库
   - 检查MySQL是否启动
   - 检查用户权限
   - 检查连接配置
   
   问题2：字符集错误
   - 确认MySQL支持utf8mb4
   - MySQL版本 >= 5.5

10. 扩展建议
    - 添加数据库版本检查
    - 添加表结构版本管理
    - 添加数据迁移功能
    - 添加备份恢复功能
"""
