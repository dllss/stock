#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
Web数据表处理模块（第十层 - Web展示层）
========================================

模块功能：
---------
本模块负责Web前端的数据表展示，处理HTTP请求并返回JSON数据。
是系统与前端交互的关键桥梁。

主要职责：
1. 处理前端对表格数据的请求
2. 从数据库查询数据
3. 数据格式转换（特别是日期和布尔值）
4. 返回JSON格式数据给前端
5. 渲染HTML页面

核心功能：
1. GetStockHtmlHandler：处理页面请求，返回HTML页面
2. GetStockDataHandler：处理数据请求，返回JSON数据
3. MyEncoder：自定义JSON编码器，处理特殊数据类型

数据流程：
前端请求 → 处理器接收 → 数据库查询 → 格式转换 → JSON返回

实战应用：
- 用户访问某个表格页面
- 前端通过AJAX请求数据
- 此模块处理请求并返回数据
- 前端展示数据

性能优化建议：
1. 添加分页功能（limit offset）
2. 添加缓存机制（减少数据库查询）
3. 添加异步任务队列（处理大量数据）
4. 添加请求速率限制（防止恶意请求）
5. 添加数据压缩（减少网络传输）
"""

# ==================== 导入必需的库 ====================
import json  # JSON序列化和反序列化
from abc import ABC  # 抽象基类，用于定义接口
from tornado import gen  # Tornado异步生成器装饰器
import datetime  # 日期时间处理
# import logging  # 日志记录（注释状态，需要时打开）

# ==================== 导入项目模块 ====================
import instock.lib.trade_time as trd  # 交易时间工具
import instock.core.singleton_stock_web_module_data as sswmd  # Web模块数据单例
import instock.web.base as webBase  # Web基础类

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 自定义JSON编码器 ====================

class MyEncoder(json.JSONEncoder):
    """
    自定义JSON编码器 - 处理特殊数据类型
    
    功能说明：
    JSON标准库不能直接序列化 bytes 和 datetime 类型
    此编码器扩展了JSONEncoder，可以处理这些特殊类型
    
    处理的数据类型：
    1. bytes：转换为"是"或"否"（1为是，0为否）
    2. datetime.date：转换为OADate格式（Excel兼容）
    3. 其他：使用默认编码器处理
    
    使用示例：
    --------
    data = {
        'is_active': b'\x01',  # bytes
        'trade_date': datetime.date(2024, 1, 1),  # 日期
    }
    json_str = json.dumps(data, cls=MyEncoder)
    # 结果：{'is_active': '是', 'trade_date': '/OADate(45292.0)/'}
    """

    def default(self, obj):
        """
        重写default方法 - 处理不能序列化的对象
        
        参数说明：
        ---------
        obj: 要序列化的对象
        
        返回值：
        -------
        JSON可序列化的值
        
        执行步骤：
        --------
        1. 检查是否为 bytes 类型
        2. 检查是否为 datetime.date 类型
        3. 其他类型使用默认处理
        
        特殊说明：
        --------
        OADate是Excel中使用的日期格式
        是从1899年12月30日到目标日期的天数
        """
        if isinstance(obj, bytes):
            # bytes类型处理：1表示是，0表示否
            # ord()：获取字节的ASCII值
            return "是" if ord(obj) == 1 else "否"
        elif isinstance(obj, datetime.date):
            # datetime.date类型处理：转换为OADate格式
            # 步骤1：将date转换为datetime（加上00:00:00时间）
            dt = datetime.datetime.combine(obj, datetime.time.min)
            # 步骤2：计算与1899-12-30的天数差
            delta = dt - datetime.datetime(1899, 12, 30)
            # 步骤3：转换为OADate格式字符串
            # delta.days：完整天数
            # delta.seconds / 86400：小数部分（秒数转换为天的小数）
            # 86400 = 24 * 60 * 60（一天的秒数）
            oa_date = float(delta.days) + (float(delta.seconds) / 86400)
            return f'/OADate({oa_date})/'
        else:
            # 其他类型使用默认JSONEncoder处理
            return json.JSONEncoder.default(self, obj)


# ==================== 页面处理器 ====================

class GetStockHtmlHandler(webBase.BaseHandler, ABC):
    """
    获取股票数据表页面处理器
    
    功能说明：
    ---------
    处理用户访问表格页面的请求
    返回渲染后的HTML页面，包含表格和左侧菜单
    
    HTTP请求：
    ----------
    GET /stock_table?table_name=cn_stock_selection
    
    请求参数：
    ---------
    table_name (str): 表名称，如'cn_stock_selection'
    
    返回内容：
    --------
    HTML页面，包含：
    1. 表格数据容器
    2. 左侧导航菜单
    3. 当前交易日期
    4. 表格配置信息
    
    工作流程：
    --------
    1. 从URL获取table_name参数
    2. 获取表的配置信息（列名、排序等）
    3. 获取当前交易日期
    4. 判断是否实时数据
    5. 渲染HTML模板
    
    前端交互流程：
    -----------
    1. 用户打开页面
    2. 页面加载HTML框架
    3. 通过AJAX请求GetStockDataHandler获取数据
    4. 动态渲染表格内容
    5. 用户可以切换日期、排序等
    
    性能考虑：
    ---------
    - 分离页面渲染和数据查询
    - 前端AJAX实现动态加载
    - 减少服务器压力
    """
    
    @gen.coroutine
    def get(self):
        """
        处理GET请求 - 返回HTML页面
        
        执行步骤：
        --------
        1. 获取table_name参数
        2. 从单例获取表配置
        3. 获取最后一个交易日期
        4. 构建当前日期字符串
        5. 渲染HTML模板
        """
        # 步骤1：获取请求参数
        # get_argument：从URL参数中获取值
        # default=None：未提供参数时使用None
        # strip=False：不删除前后空格
        name = self.get_argument("table_name", default=None, strip=False)
        
        # 步骤2：获取表的配置信息
        # sswmd.stock_web_module_data()：单例模式获取Web模块数据
        # get_data(name)：根据表名获取该表的配置
        # 配置包含：表名、列名、排序字段、是否实时等
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        
        # 步骤3：获取最后的交易日期
        # trd.get_trade_date_last()：获取最后一个交易日期
        # 返回2个值：run_date和run_date_nph
        # run_date：上个交易日
        # run_date_nph：当前自然日（非交易日时用来显示最新日期）
        run_date, run_date_nph = trd.get_trade_date_last()
        
        # 步骤4：选择合适的日期字符串
        if web_module_data.is_realtime:
            # 如果是实时数据，使用当前自然日
            date_now_str = run_date_nph.strftime("%Y-%m-%d")
        else:
            # 如果是历史数据，使用上个交易日
            date_now_str = run_date.strftime("%Y-%m-%d")
        
        # 步骤5：渲染HTML模板
        # self.render()：Tornado的模板渲染方法
        # 参数说明：
        #   - web_module_data：表配置信息
        #   - date_now：当前日期（用于前端显示）
        #   - leftMenu：左侧菜单HTML
        # self.request.uri：获取当前请求URI（用于菜单定位）
        self.render("stock_web.html", 
                    web_module_data=web_module_data, 
                    date_now=date_now_str,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))


# ==================== 数据处理器 ====================

class GetStockDataHandler(webBase.BaseHandler, ABC):
    """
    获取股票数据（JSON）处理器
    
    功能说明：
    ---------
    处理前端AJAX请求，返回JSON格式的表格数据
    是前端表格的数据源
    
    HTTP请求：
    ----------
    GET /stock_data?name=cn_stock_selection&date=2024-01-01
    
    请求参数：
    ---------
    name (str): 表名称，如'cn_stock_selection'
    date (str): 日期，格式'YYYY-MM-DD'，可选
    
    返回格式：
    --------
    JSON数组，每个元素是一行数据
    [
        {'code': '000001', 'name': '平安银行', 'price': 10.5, ...},
        {'code': '000002', 'name': '万科A', 'price': 20.3, ...},
        ...
    ]
    
    工作流程：
    --------
    1. 获取表名和日期参数
    2. 从数据库查询数据
    3. 数据格式转换（日期、布尔值等）
    4. 返回JSON数据
    
    前端使用方式：
    -----------
    // JavaScript中
    $.ajax({
        url: '/stock_data',
        data: { name: 'cn_stock_selection', date: '2024-01-01' },
        success: function(data) {
            // 数据为JSON数组
            renderTable(data);
        }
    });
    """
    
    def get(self):
        """
        处理GET请求 - 返回JSON数据
        
        执行步骤：
        --------
        1. 获取请求参数
        2. 从数据库查询数据
        3. 数据格式转换
        4. 返回JSON
        """
        # 步骤1：获取请求参数
        # name：表名称（必需）
        # date：查询日期（可选）
        name = self.get_argument("name", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        
        # 步骤2：获取表的配置信息
        # 从单例获取表的配置（列名、排序字段等）
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        
        # 步骤3：设置HTTP响应头
        # 告诉浏览器响应内容是JSON格式
        # UTF-8：字符编码（支持中文）
        self.set_header('Content-Type', 'application/json;charset=UTF-8')

        # 步骤4：构建SQL查询语句 - WHERE子句
        # 根据是否有date参数，构建不同的WHERE条件
        if date is None:
            # 没有指定日期，查询所有数据
            where = ""
        else:
            # 指定了日期，添加WHERE条件
            # 使用参数化查询（%s占位符）防止SQL注入
            where = f" WHERE `date` = %s"

        # 步骤5：构建SQL查询语句 - ORDER BY子句
        # ORDER BY用于排序查询结果
        # web_module_data.order_by：排序字段配置
        order_by = ""
        if web_module_data.order_by is not None:
            # 例如：ORDER BY price DESC, volume DESC
            order_by = f" ORDER BY {web_module_data.order_by}"

        # 步骤6：构建SQL查询语句 - 额外字段
        # 有些表需要计算额外的字段
        # 例如：涨跌幅 = (current_price - previous_price) / previous_price
        order_columns = ""
        if web_module_data.order_columns is not None:
            # 逗号加额外字段
            order_columns = f",{web_module_data.order_columns}"

        # 步骤7：完整的SQL查询语句
        # SELECT *：查询所有字段
        # order_columns：额外计算的字段
        # FROM table_name：从指定表查询
        # WHERE：条件（可选）
        # ORDER BY：排序（可选）
        # 示例：SELECT *, (price/prev_price-1)*100 FROM cn_stock_selection WHERE date='2024-01-01' ORDER BY price DESC
        sql = f" SELECT *{order_columns} FROM `{web_module_data.table_name}`{where}{order_by}"
        
        # 步骤8：执行数据库查询
        # self.db.query()：执行SQL语句
        # 参数说明：
        #   - sql：SQL语句（含占位符）
        #   - date：WHERE子句的参数值
        # 返回值：查询结果列表，每个元素是字典（行数据）
        data = self.db.query(sql, date)

        # 步骤9：将数据转换为JSON字符串并返回
        # json.dumps()：Python对象转换为JSON字符串
        # cls=MyEncoder：使用自定义编码器
        # MyEncoder处理特殊类型：bytes（是/否）、date（OADate）
        # self.write()：写入HTTP响应体
        self.write(json.dumps(data, cls=MyEncoder))


# ==================== 知识点总结 ====================
"""
核心知识点（后端开发）
====================

1. HTTP协议和REST API
   - GET请求：用于获取数据
   - URL参数传递：?name=xxx&date=yyy
   - HTTP响应头：Content-Type
   - JSON返回格式：标准API格式

2. Tornado框架
   - BaseHandler：所有处理器的基类
   - @gen.coroutine：异步处理装饰器
   - self.get_argument()：获取URL参数
   - self.write()：写入响应
   - self.render()：渲染模板

3. 数据库操作
   - 参数化查询：防止SQL注入
   - WHERE子句：条件查询
   - ORDER BY：排序
   - 从SQL结果到JSON的转换

4. 日期时间处理
   - datetime.date：日期对象
   - strftime()：日期格式化
   - OADate：Excel日期格式
   - 交易日期vs自然日期

5. JSON编码
   - 自定义编码器：处理特殊类型
   - bytes编码：字节转字符串
   - date编码：日期转OADate
   - 中文编码：UTF-8

6. 前后端交互
   - HTML模板渲染：提供页面框架
   - AJAX请求：获取数据
   - JSON数据绑定：前端处理JSON数据
   - 动态表格渲染：JavaScript处理

常见问题Q&A
===========

Q1: 为什么要分离页面和数据请求？
A: - 提高效率：多个请求可并行发送
   - 提高复用性：数据可被多个页面使用
   - 便于测试：可单独测试API
   - 支持实时刷新：前端可定时刷新数据

Q2: 参数化查询的作用？
A: - 防止SQL注入攻击
   - 提高数据库性能（查询计划缓存）
   - 示例：
     错误：sql = f"WHERE date = '{date}'" # SQL注入风险
     正确：sql = "WHERE date = %s"  # 安全

Q3: MyEncoder如何工作？
A: - JSONEncoder.default()被调用处理不能序列化的类型
   - bytes处理：ord(b'\x01') == 1
   - date处理：计算与1899-12-30的天数
   - 其他调用默认处理

Q4: OADate是什么格式？
A: - Excel中的日期格式
   - 从1899-12-30到目标日期的天数
   - 支持小数部分（时间）
   - 优点：兼容Excel导出

Q5: 如何处理时区问题？
A: - 使用datetime.date：不含时区
   - 使用datetime.datetime：含时区
   - 统一使用UTC或本地时区
   - 数据库存储时明确指定时区

Q6: 如何添加分页功能？
A: sql += f" LIMIT {page_size} OFFSET {(page-1)*page_size}"

Q7: 如何优化大数据量查询？
A: - 添加LIMIT限制返回行数
   - 添加索引加速查询
   - 考虑缓存常用查询
   - 使用异步任务队列

Q8: 前端如何调用此API？
A: // JavaScript
   $.ajax({
       url: '/stock_data?name=cn_stock_selection&date=2024-01-01',
       success: function(data) {
           // data是JSON数组
           data.forEach(row => {
               console.log(row.code, row.name);
           });
       }
   });

优化建议
=======
1. 添加请求速率限制（防止恶意请求）
2. 添加数据缓存（减少数据库查询）
3. 添加异常处理（返回统一错误格式）
4. 添加日志记录（便于调试）
5. 添加认证授权（控制访问权限）
6. 添加数据压缩（减少网络传输）
7. 添加分页功能（处理大数据）
8. 添加搜索过滤（增强功能）
"""
