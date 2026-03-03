#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
综合选股数据任务模块（第二层）
==============================
这个模块负责抓取东方财富网的综合选股数据。

什么是综合选股？
- 东方财富网提供的专业选股工具
- 包含200+个选股指标
- 涵盖基本面、技术面、消息面等
- 功能强大，数据全面

数据内容（200+个字段）：
1. 股票范围：
   - 市场、行业、地区、概念、风格
   - 指数成份、上市时间
   
2. 基本面指标：
   - 估值：市盈率、市净率、市销率
   - 每股指标：EPS、每股净资产、每股现金流
   - 盈利能力：ROE、毛利率、净利率
   - 成长能力：营收增长、净利增长
   - 偿债能力：资产负债率、流动比率
   - 股本股东：总股本、流通股、股东人数
   
3. 技术面指标：
   - 均线：MA、EMA
   - 指标：MACD、KDJ、RSI、BOLL
   - 形态：各种K线形态
   
4. 消息面指标：
   - 公告大事
   - 机构关注情况
   - 机构持股家数和比例
   
5. 人气指标：
   - 股吧人气排名
   - 粉丝数量变化
   - 浏览量排名
   
6. 行情数据：
   - 股价表现
   - 成交情况
   - 资金流向
   - 沪深股通

为什么要抓取综合选股数据？
- 数据全面：一次获取所有指标
- 来源权威：东方财富专业数据
- 方便筛选：可以自由组合条件
- 节省时间：不用单独计算每个指标

数据特点：
- 数据量大：每天约4000条记录
- 字段多：200+个字段
- 更新及时：开盘即有数据
- 质量高：专业数据源

运行时机：
- 开盘后即可运行
- 建议：上午10点后（数据更稳定）
- 或收盘后17:30
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import pandas as pd  # 数据处理
import os.path  # 路径操作
import sys  # 系统操作

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
import instock.core.stockfetch as stf  # 数据抓取模块

__author__ = 'myh '
__date__ = '2023/5/5 '


# ==================== 保存综合选股数据 ====================

"""
保存综合选股数据到数据库
参数说明：
date (datetime.date): 数据日期
before (bool): 时间标志
- True：开盘前，不执行
- False：开盘后，执行
功能说明：
1. 从东方财富网抓取综合选股数据
2. 删除数据库中的旧数据
3. 插入新数据
数据来源：
东方财富网 - 数据中心 - 综合选股
网址：http://data.eastmoney.com/xuangu/
数据内容：
200+个指标，包括：
- 基本面：估值、盈利、成长、偿债
- 技术面：均线、指标、形态
- 消息面：公告、机构
- 人气面：关注度、浏览量
- 行情面：价格、成交、资金流
执行流程：
1. 检查是否开盘
2. 抓取综合选股数据
3. 删除旧数据
4. 插入新数据
为什么删除旧数据？
- 综合选股数据会实时更新
- 多次运行需要替换为最新数据
- 主键(date, code)确保唯一性
使用场景：
- 多维度选股：结合多个指标
- 基本面选股：财务指标筛选
- 技术面选股：技术指标筛选
- Web展示：提供丰富的数据展示
"""
def save_nph_stock_selection_data(date, before=True):
    # ==================== 步骤1: 检查是否开盘 ====================
    if before:
        # 开盘前不执行
        return

    try:
        # ==================== 步骤2: 抓取综合选股数据 ====================
        # fetch_stock_selection()：从东方财富抓取数据
        # 返回DataFrame，包含200+列
        data = stf.fetch_stock_selection()
        
        # 检查数据是否有效
        if data is None:
            # 没有数据，可能网络问题
            logging.warning(f"综合选股数据抓取失败：{date}")
            return

        # ==================== 步骤3: 获取表名 ====================
        table_name = tbs.TABLE_CN_STOCK_SELECTION['name']  # 'cn_stock_selection'
        
        # ==================== 步骤4: 删除旧数据 ====================
        if mdb.checkTableIsExist(table_name):
            # 表存在，删除旧数据
            # 从数据中获取日期（第一行的date字段）
            _date = data.iloc[0]['date']
            # 构建DELETE语句
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{_date}'"
            # 执行删除
            mdb.executeSql(del_sql)
            # 表已存在，不需要指定字段类型
            cols_type = None
        else:
            # 表不存在，第一次运行
            # 获取字段类型定义
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SELECTION['columns'])

        # ==================== 步骤5: 插入新数据 ====================
        # insert_db_from_df()：从DataFrame插入数据
        # 参数：
        #   data：综合选股数据（200+列）
        #   table_name：表名
        #   cols_type：字段类型（None或dict）
        #   False：不写入索引
        #   "`date`,`code`"：主键
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        # 日志记录
        logging.info(f"保存综合选股数据成功：{date}，共{len(data)}条")
        
    except Exception as e:
        # 捕获并记录异常
        logging.error(f"selection_data_daily_job.save_nph_stock_selection_data处理异常：{e}")


# ==================== 主函数 ====================

"""
综合选股任务主函数
功能说明：
执行综合选股数据的抓取和保存
运行方式：
# 当前交易日
python selection_data_daily_job.py
# 指定日期（综合选股通常用当前数据）
python selection_data_daily_job.py
数据用途：
1. Web界面综合选股功能
2. 多维度筛选股票
3. 基本面技术面结合选股
4. 数据分析和挖掘
执行时间：
- 开盘后：可以获取实时数据
- 建议：10:00后（数据更完整）
注意事项：
- 数据量大，抓取需要时间
- 网络要稳定
- 可能被限速（使用代理）
"""
def main():
    # 执行任务
    # run_with_args()：处理命令行参数和日期
    runt.run_with_args(save_nph_stock_selection_data)
    
    # 任务完成
    logging.info("综合选股数据任务执行完成")


# ==================== 程序入口 ====================
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    
    这是一个独立的数据抓取任务
    可以单独运行，也可以被execute_daily_job调用
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
综合选股任务模块使用总结（给Python新手）
===========================================

1. 模块定位
   - 第二层：数据抓取层
   - 独立任务：可单独运行
   - 数据来源：东方财富网

2. 核心功能
   - 抓取综合选股数据
   - 200+个指标
   - 保存到数据库

3. 数据价值
   全面性：
   - 涵盖所有重要指标
   - 一次获取，多次使用
   
   权威性：
   - 东方财富专业数据
   - 数据质量高
   
   便捷性：
   - 无需单独计算
   - 直接使用现成数据

4. 指标分类
   基本面（最重要）：
   - 估值：PE、PB、PS
   - 盈利：ROE、ROA、净利率
   - 成长：营收增长、利润增长
   - 偿债：资产负债率
   
   技术面：
   - 均线系统
   - 技术指标
   - K线形态
   
   资金面：
   - 主力资金流向
   - 机构持仓
   - 北向资金
   
   情绪面：
   - 股吧人气
   - 关注度
   - 热度排名

5. 使用场景
   多维度选股：
   - PE < 20 AND ROE > 15 AND 营收增长 > 20%
   - 综合多个条件筛选
   
   基本面选股：
   - 低估值 + 高成长
   - 寻找价值股
   
   技术面选股：
   - 均线多头 + MACD金叉
   - 寻找技术机会

6. Web功能
   在Web界面：
   - 可以自由组合200+个条件
   - 实时筛选股票
   - 导出结果
   - 关注股票

7. 数据更新
   更新频率：
   - 开盘期间：实时更新
   - 收盘后：最终数据
   
   数据延迟：
   - 基本无延迟
   - 开盘即有数据

8. 注意事项
   - 数据量大（200+列）
   - 抓取耗时（1-2分钟）
   - 网络要求高
   - 可能需要代理

9. Python知识点
   - 数据抓取：网络请求
   - DataFrame操作：pandas
   - 数据库操作：SQL
   - 异常处理：try-except

10. 对比其他数据
    vs 基础数据（cn_stock_spot）：
    - 基础数据：核心字段，约50个
    - 综合数据：全部字段，200+个
    - 综合数据包含基础数据
    
    选择使用：
    - 简单查询：用基础数据（快）
    - 复杂选股：用综合数据（全）
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import pandas as pd  # 数据处理
import os.path  # 路径操作
import sys  # 系统操作

# ==================== 路径配置 ====================
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# ==================== 导入项目模块 ====================
import instock.lib.run_template as runt  # 任务运行模板
import instock.core.tablestructure as tbs  # 表结构定义
import instock.lib.database as mdb  # 数据库操作
import instock.core.stockfetch as stf  # 数据抓取模块

__author__ = 'myh '
__date__ = '2023/5/5 '


# ==================== 保存综合选股数据 ====================

"""
保存综合选股数据到数据库
参数说明：
date (datetime.date): 数据日期
before (bool): 时间标志
功能说明：
从东方财富网抓取并保存综合选股数据
执行流程：
1. 检查是否开盘
2. 抓取数据
3. 删除旧数据
4. 插入新数据
"""
def save_nph_stock_selection_data(date, before=True):
    # 开盘前不执行
    if before:
        return

    try:
        # ==================== 步骤1: 抓取综合选股数据 ====================
        # fetch_stock_selection()：从东方财富网抓取
        # 返回DataFrame，包含所有股票的200+个指标
        data = stf.fetch_stock_selection()
        
        # 检查数据有效性
        if data is None:
            logging.warning(f"综合选股数据抓取失败：{date}")
            return

        # ==================== 步骤2: 获取表名 ====================
        table_name = tbs.TABLE_CN_STOCK_SELECTION['name']  # 'cn_stock_selection'
        
        # ==================== 步骤3: 删除旧数据 ====================
        if mdb.checkTableIsExist(table_name):
            # 表存在，删除该日期的旧数据
            # 从第一行数据中提取日期
            _date = data.iloc[0]['date']
            # 构建DELETE语句
            del_sql = f"DELETE FROM `{table_name}` where `date` = '{_date}'"
            # 执行删除
            mdb.executeSql(del_sql)
            # 表已存在，不需要指定字段类型
            cols_type = None
        else:
            # 表不存在，第一次运行
            # 获取字段类型定义（200+个字段）
            cols_type = tbs.get_field_types(tbs.TABLE_CN_STOCK_SELECTION['columns'])

        # ==================== 步骤4: 插入新数据 ====================
        # 插入DataFrame到数据库
        # 主键：date和code组合
        mdb.insert_db_from_df(data, table_name, cols_type, False, "`date`,`code`")
        
        # 记录成功日志
        logging.info(f"保存综合选股数据成功：{date}，共{len(data)}条，{len(data.columns)}个字段")
        
    except Exception as e:
        # 捕获并记录异常
        logging.error(f"selection_data_daily_job.save_nph_stock_selection_data处理异常：{e}")


# ==================== 主函数 ====================

"""
综合选股任务主函数
执行综合选股数据的抓取任务
运行方式：
python selection_data_daily_job.py
"""
def main():
    # 执行任务
    # run_with_args()：处理命令行参数
    # 自动判断交易日，支持批量执行
    runt.run_with_args(save_nph_stock_selection_data)
    
    # 任务完成
    logging.info("综合选股数据任务执行完成")


# ==================== 程序入口 ====================
if __name__ == '__main__':
    """
    直接运行此脚本时的入口
    """
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()
    main()


"""
===========================================
综合选股任务模块使用总结（给Python新手）
===========================================

1. 核心功能
   - 抓取综合选股数据
   - 200+个维度指标
   - 一站式数据获取

2. 数据价值
   - 节省计算：不用自己算
   - 数据全面：涵盖所有维度
   - 来源权威：东方财富专业
   - 更新及时：开盘即有

3. 应用场景
   - 多因子选股
   - 基本面分析
   - 技术面分析
   - 量化研究

4. 使用建议
   - 开盘后运行
   - 选择合适指标
   - 组合使用
   - 回测验证

5. 注意事项
   - 抓取耗时较长
   - 数据量大
   - 注意网络
   - 可能被限速
"""
