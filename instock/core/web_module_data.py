#!/usr/local/bin/python
# -*- coding: utf-8 -*-
"""
Web模块数据配置类
==================
这个类用于配置Web界面中的数据展示模块。

什么是数据展示模块？
- 在Web界面上，每个数据表都有一个展示页面
- 如：每日股票数据、资金流向、龙虎榜等
- 这个类定义了如何展示这些数据

配置内容：
- 模块名称和图标
- 数据来源（哪个数据库表）
- 显示哪些列
- 如何排序
- 是否实时更新

使用场景：
- singleton_stock_web_module_data.py中定义所有模块配置
- Web服务根据配置动态生成页面
- 添加新功能只需要添加配置，不需要写前端代码

设计思想：
- 配置化：通过配置而不是代码来控制展示
- 可扩展：添加新模块非常简单
- 统一化：所有模块使用相同的展示逻辑
"""

__author__ = 'myh '
__date__ = '2023/5/11 '


class web_module_data:
    """
    Web数据展示模块配置类
    
    这个类封装了Web界面中一个数据模块的所有配置信息
    
    属性说明：
        mode (str): 模块模式
            - 'query'：查询模式，只读展示
            - 'editor'：编辑模式，可以修改数据
            
        type (str): 模块类型
            - 用于分类和导航
            - 如：'stock'（股票数据）、'etf'（ETF数据）等
            
        ico (str): 图标类名
            - 在左侧菜单显示的图标
            - 使用Font Awesome图标库
            - 如：'fa-line-chart'、'fa-table'等
            
        name (str): 模块显示名称
            - 在菜单和页面标题显示
            - 如：'每日股票数据'、'资金流向'等
            
        table_name (str): 数据库表名
            - 数据来源的数据库表
            - 如：'cn_stock_spot'、'cn_stock_fund_flow'
            
        columns (list): 数据库列名列表
            - 数据库表的实际列名
            - 如：['code', 'name', 'new_price', ...]
            
        column_names (list): 列的显示名称列表
            - 对应columns，用于界面显示
            - 如：['代码', '名称', '最新价', ...]
            - 必须与columns长度相同
            
        primary_key (list): 主键列表
            - 数据库表的主键字段
            - 用于唯一标识每一行数据
            - 如：['date', 'code']
            
        is_realtime (bool): 是否实时数据
            - True：开盘时数据实时变化，可以实时刷新
            - False：历史数据，不需要实时刷新
            
        order_columns (list, 可选): 排序列索引列表
            - 指定按哪些列排序
            - 如：[0, 3] 表示按第1列和第4列排序
            - None表示不指定排序列
            
        order_by (str, 可选): 排序方式
            - 'asc'：升序（从小到大）
            - 'desc'：降序（从大到小）
            - None表示使用默认排序
            
        url (str): 数据接口URL（自动生成）
            - Web前端通过这个URL获取数据
            - 格式：/instock/data?table_name=表名
            
    使用示例：
        # 配置每日股票数据模块
        stock_spot_module = web_module_data(
            mode='query',  # 查询模式
            type='stock',  # 股票类型
            ico='fa-line-chart',  # 图表图标
            name='每日股票数据',  # 模块名称
            table_name='cn_stock_spot',  # 数据表名
            columns=['code', 'name', 'new_price', 'change_rate'],  # 数据库列名
            column_names=['代码', '名称', '最新价', '涨跌幅'],  # 显示名称
            primary_key=['date', 'code'],  # 主键
            is_realtime=True,  # 实时数据
            order_columns=[3],  # 按第4列排序（涨跌幅）
            order_by='desc'  # 降序
        )
        
        # 访问配置
        print(stock_spot_module.name)  # 输出：每日股票数据
        print(stock_spot_module.url)   # 输出：/instock/data?table_name=cn_stock_spot
        
    配置流程：
        1. 在singleton_stock_web_module_data.py中创建配置
        2. Web服务启动时加载所有配置
        3. 根据配置生成左侧菜单
        4. 用户点击菜单，根据配置加载数据
        5. 前端根据配置显示数据表格
    """
    
    def __init__(self, mode, type, ico, name, table_name, columns, column_names, 
                 primary_key, is_realtime, order_columns=None, order_by=None):
        """
        初始化Web模块配置
        
        参数说明：
            见类的属性说明
            
        执行流程：
            1. 保存所有配置参数
            2. 自动生成数据接口URL
            
        注意事项：
            - columns和column_names必须长度相同
            - columns是英文，column_names是中文
            - primary_key中的字段必须在columns中
        """
        self.mode = mode  # 模式：query（查询）或 editor（编辑）
        self.type = type  # 类型：用于分类
        self.ico = ico  # 图标：Font Awesome类名
        self.name = name  # 名称：显示在界面上
        self.table_name = table_name  # 表名：数据来源
        self.columns = columns  # 列名：数据库字段
        self.column_names = column_names  # 列显示名：中文名称
        self.primary_key = primary_key  # 主键：唯一标识
        self.is_realtime = is_realtime  # 是否实时：决定是否需要刷新
        self.order_by = order_by  # 排序方式：asc或desc
        self.order_columns = order_columns  # 排序列：列索引列表
        
        # 自动生成数据接口URL
        # f-string：格式化字符串
        # 前端JavaScript会请求这个URL获取数据
        self.url = f"/instock/data?table_name={self.table_name}"


"""
===========================================
Web模块配置类使用总结（给Python新手）
===========================================

1. 设计模式
   - 配置与逻辑分离
   - 数据驱动的界面生成
   - 一次配置，多处使用

2. 核心概念
   - 配置类：存储展示配置
   - 数据表：存储实际数据
   - Web接口：提供数据访问

3. 配置内容
   - 显示配置：名称、图标
   - 数据配置：表名、列名
   - 排序配置：排序列、排序方式
   - 行为配置：是否实时、是否可编辑

4. 使用流程
   创建配置 → 加载到单例 → Web服务读取 → 生成界面

5. 扩展方法
   - 添加新模块：只需添加配置
   - 修改显示：修改配置即可
   - 无需修改前端代码

6. 相关文件
   - web_module_data.py：配置类定义（本文件）
   - singleton_stock_web_module_data.py：所有模块配置
   - web_service.py：Web服务读取配置
   - dataTableHandler.py：根据配置处理数据请求

7. Python知识点
   - 类：封装相关数据和方法
   - self：实例本身的引用
   - f-string：格式化字符串（Python 3.6+）
   - 可选参数：=None表示参数可选

8. 前端对应
   - 配置 → HTML表格
   - columns → 表头
   - data → 表格内容
   - order_by → 排序按钮
"""
