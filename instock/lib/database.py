#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库操作模块 - 核心基础模块
===========================
这个文件是整个系统的数据库操作基础，提供了与MySQL数据库交互的所有功能。
主要功能：
1. 数据库连接配置
2. 数据插入（从pandas DataFrame）
3. 数据更新
4. 数据查询
5. 表存在性检查

重要提示：对于Python新手
- import 语句：导入其他模块或库，让我们可以使用它们的功能
- 变量赋值：使用 = 号给变量赋值
- 函数定义：使用 def 关键字定义函数
- 异常处理：使用 try-except 捕获和处理错误
"""

# ==================== 导入必需的库 ====================
import logging  # logging：日志记录库，用于记录程序运行信息和错误
import os  # os：操作系统接口库，用于读取环境变量等系统信息
import pymysql  # pymysql：Python的MySQL数据库驱动，用于直接操作MySQL
from sqlalchemy import create_engine  # create_engine：SQLAlchemy的核心函数，用于创建数据库引擎
from sqlalchemy.types import NVARCHAR  # NVARCHAR：数据库字段类型，表示可变长度的Unicode字符串
from sqlalchemy import inspect  # inspect：用于检查数据库表的结构（如主键、索引等）

# ==================== 作者和日期信息 ====================
__author__ = 'myh '
__date__ = '2023/3/10 '

# ==================== 数据库配置参数 ====================
# 这些是数据库的默认配置，可以根据实际情况修改
db_host = "localhost"  # 数据库服务器地址，localhost表示本机
db_user = "root"  # 数据库登录用户名
db_password = "root"  # 数据库登录密码（实际使用时应该设置更安全的密码）
db_database = "instockdb"  # 要使用的数据库名称
db_port = 3306  # MySQL数据库默认端口号
db_charset = "utf8mb4"  # 数据库字符集，utf8mb4支持所有Unicode字符（包括emoji等）

# ==================== 从环境变量读取配置（Docker支持）====================
# 环境变量：操作系统中的变量，可以在不修改代码的情况下改变配置
# 这对于Docker容器部署非常有用，可以通过 docker run -e 参数传递配置
_db_host = os.environ.get('db_host')  # 尝试从环境变量获取数据库主机地址
if _db_host is not None:  # 如果环境变量存在（不是None）
    db_host = _db_host  # 使用环境变量的值覆盖默认值

_db_user = os.environ.get('db_user')  # 尝试从环境变量获取数据库用户名
if _db_user is not None:
    db_user = _db_user

_db_password = os.environ.get('db_password')  # 尝试从环境变量获取数据库密码
if _db_password is not None:
    db_password = _db_password

_db_database = os.environ.get('db_database')  # 尝试从环境变量获取数据库名称
if _db_database is not None:
    db_database = _db_database

_db_port = os.environ.get('db_port')  # 尝试从环境变量获取数据库端口
if _db_port is not None:
    db_port = int(_db_port)  # 端口需要转换为整数类型

# ==================== 数据库连接字符串配置 ====================
# SQLAlchemy连接URL格式：mysql+pymysql://用户名:密码@主机:端口/数据库?charset=字符集
# %s 是字符串格式化占位符，会被后面元组中的值依次替换
MYSQL_CONN_URL = "mysql+pymysql://%s:%s@%s:%s/%s?charset=%s" % (
    db_user, db_password, db_host, db_port, db_database, db_charset)
logging.info(f"数据库链接信息：{ MYSQL_CONN_URL}")  # 记录数据库连接信息到日志

# PyMySQL的连接配置字典
# 字典（dict）：Python的键值对数据结构，用{}表示，格式为 {键: 值}
MYSQL_CONN_DBAPI = {
    'host': db_host,  # 数据库主机地址
    'user': db_user,  # 登录用户名
    'password': db_password,  # 登录密码
    'database': db_database,  # 要连接的数据库名
    'charset': db_charset,  # 字符集
    'port': db_port,  # 端口号
    'autocommit': True  # 自动提交事务，每次操作后自动保存到数据库
}

# TornDB的连接配置字典（用于Tornado web框架）
# TornDB是专门为Tornado框架设计的MySQL封装库
MYSQL_CONN_TORNDB = {
    'host': f'{db_host}:{str(db_port)}',  # f-string格式化字符串，将主机和端口组合
    'user': db_user,
    'password': db_password,
    'database': db_database,
    'charset': db_charset,
    'max_idle_time': 3600,  # 最大空闲时间（秒），超过此时间未使用的连接会被关闭
    'connect_timeout': 1000  # 连接超时时间（毫秒）
}


# ==================== 数据库引擎函数 ====================

"""
创建并返回SQLAlchemy数据库引擎（默认数据库）
什么是数据库引擎？
- 引擎是SQLAlchemy与数据库通信的核心接口
- 它管理数据库连接池，处理SQL语句的执行
- 可以把它想象成一个"数据库连接管理器"
返回值：
SQLAlchemy Engine对象，用于执行数据库操作
"""
def engine():
    return create_engine(MYSQL_CONN_URL)


"""
创建连接到指定数据库的引擎
参数：
to_db (str): 目标数据库名称
返回值：
SQLAlchemy Engine对象，连接到指定数据库
使用场景：
当需要操作不同的数据库时使用（比如备份、迁移数据）
"""
def engine_to_db(to_db):
    # 将连接URL中的默认数据库名替换为目标数据库名
    _engine = create_engine(MYSQL_CONN_URL.replace(f'/{db_database}?', f'/{to_db}?'))
    return _engine


# ==================== 数据库连接函数 ====================

"""
获取PyMySQL数据库连接对象
什么是数据库连接？
- 连接是程序与数据库之间的通道
- 通过连接可以执行SQL语句，进行增删改查操作
返回值：
pymysql.connections.Connection对象，如果连接失败则返回None
异常处理：
使用try-except捕获连接错误，并记录到日志
注意：
这个函数使用PyMySQL的原生连接，而不是SQLAlchemy
适用于需要直接执行SQL语句的场景
"""
def get_connection():
    try:
        # **MYSQL_CONN_DBAPI 是Python的字典解包语法
        # 将字典中的键值对作为函数的关键字参数传递
        # 等同于：pymysql.connect(host=..., user=..., password=..., ...)
        return pymysql.connect(**MYSQL_CONN_DBAPI)
    except Exception as e:  # Exception是所有异常的基类，可以捕获任何错误
        # 记录错误日志，包含配置信息和错误详情
        logging.error(f"database.conn_not_cursor处理异常：{MYSQL_CONN_DBAPI}{e}")
    return None  # 连接失败时返回None


# ==================== 数据插入函数 ====================

"""
从pandas DataFrame插入数据到默认数据库（最常用的插入函数）
pandas DataFrame是什么？
- DataFrame是pandas库的核心数据结构，类似于Excel表格
- 它有行和列，可以存储不同类型的数据
- 在这个系统中，所有的股票数据都先存储在DataFrame中，然后批量插入数据库
参数说明：
data (DataFrame): pandas数据框，包含要插入的数据
table_name (str): 数据库表名，如'cn_stock_spot'（每日股票数据）
cols_type (dict/bool/None): 字段类型定义
- None: 自动推断类型
- False/空字典: 所有字段使用NVARCHAR(255)
- dict: 指定每个字段的具体类型，如{'code': VARCHAR(6)}
write_index (bool): 是否将DataFrame的索引也写入数据库
primary_keys (str): 主键字段，如'`date`,`code`'（日期和代码组合作为主键）
indexs (dict, 可选): 额外的索引定义，用于加速查询
功能说明：
1. 插入数据到数据库表
2. 自动创建主键（如果表没有主键）
3. 自动创建索引（如果指定了indexs参数）
4. 支持重复运行（有主键保护）
使用示例：
# 插入每日股票数据
insert_db_from_df(
data=stock_df,  # 股票数据DataFrame
table_name='cn_stock_spot',  # 表名
cols_type={'code': VARCHAR(6)},  # 字段类型
write_index=False,  # 不写入索引
primary_keys='`date`,`code`'  # 主键
)
"""
def insert_db_from_df(data, table_name, cols_type, write_index, primary_keys, indexs=None):
    # 调用更通用的函数，None表示使用默认数据库
    insert_other_db_from_df(None, data, table_name, cols_type, write_index, primary_keys, indexs)


"""
从pandas DataFrame插入数据到指定数据库（通用版本）
这是一个更灵活的版本，可以指定要插入的目标数据库
参数说明：
to_db (str/None): 目标数据库名，None表示使用默认数据库
其他参数同insert_db_from_df函数
执行流程：
1. 创建数据库引擎
2. 获取表结构信息
3. 将DataFrame数据写入数据库
4. 检查并创建主键
5. 检查并创建索引
"""
def insert_other_db_from_df(to_db, data, table_name, cols_type, write_index, primary_keys, indexs=None):
    # 步骤1: 根据目标数据库创建引擎
    if to_db is None:
        engine_mysql = engine()  # 使用默认数据库
    else:
        engine_mysql = engine_to_db(to_db)  # 使用指定数据库
    
    # 步骤2: 使用inspect检查数据库表结构
    # inspect可以获取表的元数据信息（主键、索引、字段等）
    ipt = inspect(engine_mysql)
    
    # 步骤3: 获取DataFrame的所有列名
    col_name_list = data.columns.tolist()  # tolist()将pandas Index转换为Python列表
    
    # 步骤4: 如果需要写入索引，将索引名添加到列名列表的开头
    if write_index:
        col_name_list.insert(0, data.index.name)  # insert(0, x)在列表开头插入元素
    
    # 步骤5: 尝试将数据写入数据库
    try:
        if cols_type is None:
            # 情况1: 自动推断数据类型
            data.to_sql(
                name=table_name,  # 表名
                con=engine_mysql,  # 数据库引擎
                schema=to_db,  # 数据库schema
                if_exists='append',  # 如果表存在则追加数据（不覆盖）
                index=write_index  # 是否写入索引
            )
        elif not cols_type:
            # 情况2: cols_type为False或空，所有字段使用NVARCHAR(255)
            # 字典推导式：{key: value for item in list}
            data.to_sql(
                name=table_name,
                con=engine_mysql,
                schema=to_db,
                if_exists='append',
                dtype={col_name: NVARCHAR(255) for col_name in col_name_list},  # 设置所有字段为NVARCHAR(255)
                index=write_index
            )
        else:
            # 情况3: 使用指定的字段类型
            data.to_sql(
                name=table_name,
                con=engine_mysql,
                schema=to_db,
                if_exists='append',
                dtype=cols_type,  # 使用传入的类型定义
                index=write_index
            )
    except Exception as e:
        # 捕获并记录任何插入错误
        logging.error(f"database.insert_other_db_from_df处理异常：{table_name}表{e}")

    # 步骤6: 检查表是否有主键，如果没有则创建
    # 主键的作用：唯一标识每一行数据，防止重复插入
    if not ipt.get_pk_constraint(table_name)['constrained_columns']:
        try:
            # with语句：上下文管理器，确保连接和游标正确关闭
            # 即使发生错误，也会自动清理资源
            with get_connection() as conn:  # 获取数据库连接
                with conn.cursor() as db:  # 获取游标（用于执行SQL语句）
                    # 执行SQL语句：添加主键
                    # 反引号`用于包裹表名和字段名，防止与SQL关键字冲突
                    db.execute(f'ALTER TABLE `{table_name}` ADD PRIMARY KEY ({primary_keys});')
                    
                    # 步骤7: 如果指定了额外的索引，也创建它们
                    # 索引的作用：加速查询速度，特别是WHERE和JOIN操作
                    if indexs is not None:
                        for k in indexs:  # 遍历索引字典
                            # 为每个索引创建SQL语句并执行
                            db.execute(f'ALTER TABLE `{table_name}` ADD INDEX IN{k}({indexs[k]});')
        except Exception as e:
            # 记录主键或索引创建错误
            logging.error(f"database.insert_other_db_from_df处理异常：{table_name}表{e}")


# ==================== 数据更新函数 ====================

"""
从pandas DataFrame更新数据库中的数据
更新 vs 插入：
- 插入(INSERT)：添加新数据
- 更新(UPDATE)：修改已存在的数据
参数说明：
data (DataFrame): 包含要更新的数据
table_name (str): 要更新的表名
where (list): WHERE条件中使用的字段列表，用于定位要更新的行
例如：['date', 'code'] 表示根据日期和代码定位记录
功能说明：
根据where条件更新数据库中的记录
会自动处理NULL值和不同数据类型
SQL UPDATE语法：
UPDATE 表名 SET 字段1=值1, 字段2=值2 WHERE 条件字段=条件值
使用示例：
# 更新某只股票的价格信息
update_df = pd.DataFrame({
'date': ['2024-01-01'],
'code': ['600000'],
'new_price': [10.5]
})
update_db_from_df(update_df, 'cn_stock_spot', ['date', 'code'])
# 生成SQL: UPDATE cn_stock_spot SET new_price=10.5 WHERE date='2024-01-01' AND code='600000'
"""
def update_db_from_df(data, table_name, where):
    # 步骤1: 处理DataFrame中的空值
    # data.notnull()返回布尔DataFrame，标记非空值为True
    # where()方法：如果是True保持原值，如果是False替换为None
    data = data.where(data.notnull(), None)
    
    # 步骤2: 准备SQL语句的基础部分
    update_string = f'UPDATE `{table_name}` set '  # UPDATE语句的开头
    where_string = ' where '  # WHERE子句的开头
    cols = tuple(data.columns)  # 获取所有列名，转换为元组
    
    # 步骤3: 获取数据库连接并执行更新
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                # 遍历DataFrame的每一行数据
                # data.values返回numpy数组，包含所有数据值
                for row in data.values:
                    sql = update_string  # 重置SQL语句
                    sql_where = where_string  # 重置WHERE子句
                    
                    # enumerate()返回索引和值的元组：(0, 'date'), (1, 'code'), ...
                    for index, col in enumerate(cols):
                        # 判断当前字段是否是WHERE条件字段
                        if col in where:
                            # 这是WHERE条件字段，构建WHERE子句
                            if len(sql_where) == len(where_string):
                                # 第一个WHERE条件
                                if type(row[index]) == str:
                                    # 字符串类型需要加引号
                                    sql_where = f'''{sql_where}`{col}` = '{row[index]}' '''
                                else:
                                    # 数字类型不需要引号
                                    sql_where = f'''{sql_where}`{col}` = {row[index]} '''
                            else:
                                # 后续的WHERE条件，用AND连接
                                if type(row[index]) == str:
                                    sql_where = f'''{sql_where} and `{col}` = '{row[index]}' '''
                                else:
                                    sql_where = f'''{sql_where} and `{col}` = {row[index]} '''
                        else:
                            # 这是要更新的字段，构建SET子句
                            if type(row[index]) == str:
                                # 检查是否为NULL值
                                # row[index] != row[index] 是检查NaN的技巧（NaN不等于自己）
                                if row[index] is None or row[index] != row[index]:
                                    sql = f'''{sql}`{col}` = NULL, '''
                                else:
                                    sql = f'''{sql}`{col}` = '{row[index]}', '''
                            else:
                                # 数字类型
                                if row[index] is None or row[index] != row[index]:
                                    sql = f'''{sql}`{col}` = NULL, '''
                                else:
                                    sql = f'''{sql}`{col}` = {row[index]}, '''
                    
                    # 步骤4: 组合完整的SQL语句
                    # sql[:-2] 去掉最后的逗号和空格
                    sql = f'{sql[:-2]}{sql_where}'
                    
                    # 步骤5: 执行更新语句
                    db.execute(sql)
            except Exception as e:
                # 记录更新错误，包含SQL语句便于调试
                logging.error(f"database.update_db_from_df处理异常：{sql}{e}")


# ==================== 数据库查询和工具函数 ====================

"""
检查数据库表是否存在
参数说明：
tableName (str): 要检查的表名
返回值：
bool: True表示表存在，False表示不存在
实现原理：
查询MySQL的元数据库information_schema
information_schema存储了数据库的所有表结构信息
使用场景：
在创建表前检查表是否已存在
避免重复创建导致错误
使用示例：
if checkTableIsExist('cn_stock_spot'):
print("表已存在")
else:
print("表不存在，需要创建")
"""
def checkTableIsExist(tableName):
    with get_connection() as conn:
        with conn.cursor() as db:
            # 查询information_schema.tables表
            # COUNT(*)统计符合条件的记录数
            db.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = '{0}'
                """.format(tableName.replace('\'', '\'\'')))  # 转义单引号，防止SQL注入
            
            # fetchone()获取一行结果，返回元组
            # [0]取第一个元素，即COUNT(*)的值
            if db.fetchone()[0] == 1:
                return True  # 找到1条记录，表示表存在
    return False  # 没找到，表不存在


"""
执行SQL语句（增删改操作，不返回结果）
参数说明：
sql (str): 要执行的SQL语句
可以使用参数占位符%s，如"INSERT INTO table VALUES (%s, %s)"
params (tuple): SQL语句的参数，默认为空元组
例如：("value1", "value2")
功能说明：
执行INSERT、UPDATE、DELETE等不需要返回结果的SQL语句
自动处理异常并记录日志
参数化查询的好处：
1. 防止SQL注入攻击
2. 自动处理数据类型转换
3. 自动转义特殊字符
使用示例：
# 不带参数
executeSql("DELETE FROM cn_stock_spot WHERE date < '2020-01-01'")
# 带参数（推荐）
executeSql(
"INSERT INTO cn_stock_spot (date, code, name) VALUES (%s, %s, %s)",
('2024-01-01', '600000', '浦发银行')
)
"""
def executeSql(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                # 执行SQL语句
                # params会自动替换SQL中的%s占位符
                db.execute(sql, params)
            except Exception as e:
                # 记录错误，包含SQL语句便于调试
                logging.error(f"database.executeSql处理异常：{sql}{e}")


"""
执行SQL查询并返回所有结果
参数说明：
sql (str): SELECT查询语句
params (tuple): 查询参数，默认为空元组
返回值：
tuple: 查询结果，每一行是一个元组，所有行组成一个大元组
例如：(('600000', '浦发银行'), ('600001', '邯郸钢铁'))
None: 查询出错时返回None
功能说明：
执行SELECT查询并返回所有匹配的记录
适用于结果集不大的查询
fetchall() vs fetchone():
- fetchall(): 返回所有结果行
- fetchone(): 只返回一行结果
使用示例：
# 查询所有股票代码和名称
results = executeSqlFetch(
"SELECT code, name FROM cn_stock_spot WHERE date = %s",
('2024-01-01',)  # 注意：单个参数也要用元组，末尾加逗号
)
# 遍历结果
if results:
for row in results:
code, name = row
print(f"代码：{code}，名称：{name}")
"""
def executeSqlFetch(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                # 执行查询
                db.execute(sql, params)
                # 返回所有结果
                return db.fetchall()
            except Exception as e:
                # 记录错误
                logging.error(f"database.executeSqlFetch处理异常：{sql}{e}")
    return None  # 出错时返回None


"""
执行COUNT查询并返回数量
参数说明：
sql (str): 包含COUNT()的SQL查询语句
params (tuple): 查询参数，默认为空元组
返回值：
int: 统计数量，出错时返回0
功能说明：
专门用于执行计数查询
自动从结果中提取数字
SQL COUNT函数：
COUNT(*): 统计所有行数（包括NULL）
COUNT(column): 统计某列非NULL的行数
COUNT(DISTINCT column): 统计某列不重复值的个数
使用示例：
# 统计某天的股票总数
count = executeSqlCount(
"SELECT COUNT(*) FROM cn_stock_spot WHERE date = %s",
('2024-01-01',)
)
print(f"共有{count}只股票")
# 统计价格大于100的股票数
high_price_count = executeSqlCount(
"SELECT COUNT(*) FROM cn_stock_spot WHERE new_price > %s",
(100,)
)
"""
def executeSqlCount(sql, params=()):
    with get_connection() as conn:
        with conn.cursor() as db:
            try:
                # 执行查询
                db.execute(sql, params)
                # 获取所有结果
                result = db.fetchall()
                
                # 检查结果
                if len(result) == 1:
                    # COUNT查询通常返回一行一列
                    # result[0]是第一行（唯一一行）
                    # result[0][0]是这一行的第一列（COUNT的值）
                    return int(result[0][0])
                else:
                    # 异常情况，返回0
                    return 0
            except Exception as e:
                # 记录错误
                logging.error(f"database.select_count计算数量处理异常：{e}")
    return 0  # 出错时返回0


"""
===========================================
数据库模块使用总结（给Python新手）
===========================================

1. 数据库配置
   - 修改文件开头的db_*变量设置数据库连接
   - 或使用Docker环境变量

2. 主要功能函数
   - insert_db_from_df(): 插入DataFrame数据【最常用】
   - update_db_from_df(): 更新DataFrame数据
   - executeSqlFetch(): 查询数据
   - executeSqlCount(): 统计数量
   - checkTableIsExist(): 检查表是否存在

3. 数据流程
   网络抓取 → pandas DataFrame → 数据库
   ↓
   数据清洗/计算
   ↓
   Web展示

4. 注意事项
   - 所有数据库操作都有异常处理
   - 使用参数化查询防止SQL注入
   - 自动管理数据库连接（with语句）
   - 错误信息会记录到日志文件

5. 调试技巧
   - 查看日志文件：stock_execute_job.log
   - 使用Navicat查看数据库内容
   - 打印SQL语句进行调试

"""
