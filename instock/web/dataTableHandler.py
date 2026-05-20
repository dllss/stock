#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
Web数据处理模块 - 股票数据表格处理器
=======================================

功能说明：
本模块负责处理Web端的股票数据请求，提供两种主要功能：
1. 生成股票数据页面的HTML内容
2. 返回股票数据的JSON格式（用于前端AJAX请求）

核心组件：
- MyEncoder：自定义JSON编码器，处理特殊数据类型
- GetStockHtmlHandler：获取页面HTML的处理器
- GetStockDataHandler：获取JSON数据的处理器

数据流程：
1. 前端请求 → Tornado路由 → Handler处理
2. Handler从数据库查询数据
3. 数据编码为JSON或渲染为HTML
4. 返回给前端展示

使用场景：
- 用户访问股票数据页面
- 前端通过AJAX加载数据表格
- 实时数据更新和展示

技术要点：
- Tornado框架的异步处理（@gen.coroutine）
- 自定义JSON序列化（处理日期、二进制等）
- SQL动态构建（WHERE、ORDER BY）
- 单例模式获取Web模块配置

注意事项：
- 所有SQL查询都使用参数化查询，防止SQL注入
- 日期格式需要特殊处理（OADate格式）
- 实时数据和历史数据的日期处理不同

依赖关系：
- tornado：Web框架
- instock.web.base：基础Handler类
- instock.core.singleton_stock_web_module_data：Web模块配置
- instock.lib.trade_time：交易时间工具
"""

import json
from abc import ABC
from tornado import gen
import logging
import datetime
import instock.lib.trade_time as trd
import instock.core.singleton_stock_web_module_data as sswmd
import instock.web.base as webBase

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 自定义JSON编码器 ====================

"""
MyEncoder - 自定义JSON编码器
功能：处理标准JSON无法序列化的特殊数据类型

为什么需要自定义编码器？
- 标准json.JSONEncoder只能处理基本类型（str, int, float, bool, None, list, dict）
- 我们的数据中包含：
  1. bytes类型：表示是否标志（如"是否涨停"字段）
  2. datetime.date类型：日期对象
  
解决方案：
- bytes → 转换为中文"是"/"否"
- date → 转换为Excel OADate格式（JavaScript可识别）

OADate格式说明：
- Excel内部使用的一种日期表示方法
- 基准日期：1899年12月30日
- 计算方式：从基准日期到目标日期的天数
- JavaScript端可以通过/OADate(...)/格式识别并转换

使用示例：
```python
data = {'name': '测试', 'is_limit_up': b'\x01', 'date': datetime.date(2024, 1, 1)}
json_str = json.dumps(data, cls=MyEncoder)
# 结果：{"name": "测试", "is_limit_up": "是", "date": "/OADate(45292.0)/"}
```
"""
class MyEncoder(json.JSONEncoder):
    """
    重写default方法，处理特殊类型的序列化
    
    参数：
    obj (any): 需要序列化的对象
    
    返回：
    any: 序列化后的值
    
    处理逻辑：
    1. 如果是bytes类型：
       - ord(b'\x01') == 1 → "是"
       - ord(b'\x00') == 0 → "否"
    2. 如果是datetime.date类型：
       - 计算与1899-12-30的天数差
       - 转换为OADate格式字符串
    3. 其他类型：调用父类的默认处理
    """
    def default(self, obj):
        # 处理bytes类型：数据库中的布尔标志
        if isinstance(obj, bytes):
            # ord()函数将字节转换为整数
            # b'\x01' → 1 → "是"
            # b'\x00' → 0 → "否"
            return "是" if ord(obj) == 1 else "否"
        
        # 处理日期类型：转换为Excel OADate格式
        elif isinstance(obj, datetime.date):
            # 计算从1899-12-30到目标日期的天数
            # 这是Excel使用的日期基准
            delta = datetime.datetime.combine(obj, datetime.time.min) - datetime.datetime(1899, 12, 30)
            
            # 转换为浮点数格式：天数 + 秒数/86400
            # 86400 = 24 * 60 * 60（一天的秒数）
            oadate_value = float(delta.days) + (float(delta.seconds) / 86400)
            
            # 返回OADate格式字符串，JavaScript端会识别这种格式
            return f'/OADate({oadate_value})/'
            
            # 备选方案：直接返回ISO格式字符串
            # return obj.isoformat()
        
        # 其他类型：调用父类的默认处理方法
        else:
            return json.JSONEncoder.default(self, obj)


# ==================== HTML页面处理器 ====================

"""
GetStockHtmlHandler - 股票数据页面HTML处理器
继承关系：webBase.BaseHandler + ABC（抽象基类）

功能：
根据请求参数生成股票数据页面的HTML内容

请求参数：
- table_name：数据表名称（如"stock_zh_a_spot_em"）

执行流程：
1. 接收table_name参数
2. 获取对应的Web模块配置
3. 判断是否为实时数据，确定显示日期
4. 渲染stock_web.html模板
5. 返回HTML页面

使用场景：
用户直接在浏览器访问：
http://localhost:9988/stock_web?table_name=stock_zh_a_spot_em

返回值：
渲染后的HTML页面，包含：
- 股票数据表格
- 日期选择器
- 左侧菜单
- 页面布局
"""
class GetStockHtmlHandler(webBase.BaseHandler, ABC):
    """
    处理GET请求，生成股票数据页面
    
    URL示例：
    /stock_web?table_name=stock_zh_a_spot_em
    
    步骤说明：
    1. 获取参数：从URL中提取table_name
    2. 获取配置：从单例中获取该表的配置信息
    3. 确定日期：
       - 实时数据：使用非停牌日期
       - 历史数据：使用最新交易日
    4. 渲染模板：传入配置和日期，生成HTML
    """
    @gen.coroutine
    def get(self):
        """
        Tornado异步GET请求处理方法
        
        @gen.coroutine装饰器：
        - 使方法支持异步操作
        - 可以配合yield使用，提高并发性能
        - 在Tornado中处理大量并发请求时非常有用
        
        注意：虽然这个方法没有使用yield，但保留装饰器以保持一致性
        """
        
        # ==================== 步骤1: 获取请求参数 ====================
        # 从URL参数中获取表名
        # 示例：?table_name=stock_zh_a_spot_em
        name = self.get_argument("table_name", default=None, strip=False)
        
        # ==================== 步骤2: 获取Web模块配置 ====================
        # 通过单例模式获取该表的配置信息
        # 配置包括：表名、列定义、是否实时数据、排序规则等
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        
        # ==================== 步骤3: 获取交易日期 ====================
        # 获取最新的交易日期和非停牌日期
        # run_date：最新交易日
        # run_date_nph：最新非停牌日期（用于实时数据）
        run_date, run_date_nph = trd.get_trade_date_last()
        
        # ==================== 步骤4: 确定显示的日期 ====================
        # 优先从URL参数中获取date，如果没有则使用默认日期
        date_param = self.get_argument("date", default=None, strip=False)
        
        if date_param:
            # 如果URL中有date参数，使用它（保留用户之前选择的日期）
            date_now_str = date_param
            logging.info(f"✅ 从URL获取date参数: {date_param}")
        elif web_module_data.is_realtime:
            # 实时数据：使用非停牌日期
            # 原因：实时行情可能包含停牌股票，需要用非停牌日期
            date_now_str = run_date_nph.strftime("%Y-%m-%d")
            logging.info(f"⚠️  URL无date参数，使用非停牌日期: {date_now_str}")
        else:
            # 历史数据：使用最新交易日
            date_now_str = run_date.strftime("%Y-%m-%d")
            logging.info(f"⚠️  URL无date参数，使用最新交易日: {date_now_str}")
        
        # ==================== 步骤5: 渲染HTML模板 ====================
        # 使用Tornado模板引擎渲染页面
        # 传入的参数会在模板中使用
        self.render("stock_web.html", 
                    web_module_data=web_module_data,  # Web模块配置
                    date_now=date_now_str,             # 当前日期
                    leftMenu=webBase.GetLeftMenu(self.request.uri))  # 左侧菜单


# ==================== JSON数据处理器 ====================

"""
GetStockDataHandler - 股票数据JSON处理器
继承关系：webBase.BaseHandler + ABC（抽象基类）

功能：
根据请求参数从数据库查询股票数据，返回JSON格式

请求参数：
- name：数据表名称
- date：查询日期（可选）

执行流程：
1. 接收name和date参数
2. 获取对应的Web模块配置
3. 构建SQL查询语句（动态添加WHERE和ORDER BY）
4. 执行数据库查询
5. 将结果序列化为JSON（使用MyEncoder处理特殊类型）
6. 返回JSON数据

使用场景：
前端通过AJAX请求数据：
$.ajax({
    url: '/stock_data',
    data: {name: 'stock_zh_a_spot_em', date: '2024-01-01'},
    success: function(data) {
        // 处理返回的股票数据
    }
});

SQL构建示例：
无日期：SELECT * FROM `stock_zh_a_spot_em` ORDER BY `code`
有日期：SELECT * FROM `stock_zh_a_spot_em` WHERE `date` = '2024-01-01' ORDER BY `code`
有排序：SELECT *, extra_column FROM `table_name` ORDER BY `column`

安全特性：
- 使用参数化查询（%s占位符），防止SQL注入
- 表名和列名来自配置，不由用户直接控制
"""
class GetStockDataHandler(webBase.BaseHandler, ABC):
    """
    处理GET请求，返回股票数据的JSON格式
    
    URL示例：
    /stock_data?name=stock_zh_a_spot_em&date=2024-01-01
    
    返回格式：
    Content-Type: application/json;charset=UTF-8
    Body: [{"code": "000001", "name": "平安银行", ...}, ...]
    
    步骤说明：
    1. 获取参数：从URL中提取name和date
    2. 获取配置：从单例中获取该表的配置信息
    3. 设置响应头：声明返回JSON格式
    4. 构建SQL：根据配置动态添加WHERE和ORDER BY
    5. 执行查询：使用参数化查询防止SQL注入
    6. 返回JSON：使用MyEncoder处理特殊类型
    """
    def get(self):
        """
        GET请求处理方法
        
        注意：这里没有使用@gen.coroutine，因为是同步查询
        对于简单的数据库查询，同步方式已经足够快
        """
        
        # ==================== 步骤1: 获取请求参数 ====================
        # 从URL参数中获取表名和日期
        # 示例：?name=stock_zh_a_spot_em&date=2024-01-01
        name = self.get_argument("name", default=None, strip=False)
        date = self.get_argument("date", default=None, strip=False)
        
        # ==================== 步骤2: 获取Web模块配置 ====================
        # 通过单例模式获取该表的配置信息
        web_module_data = sswmd.stock_web_module_data().get_data(name)
        
        # ==================== 步骤3: 设置响应头 ====================
        # 声明返回的内容类型为JSON，编码为UTF-8
        # 这样浏览器和前端框架能正确解析
        self.set_header('Content-Type', 'application/json;charset=UTF-8')

        # ==================== 步骤4: 构建WHERE条件 ====================
        # 如果提供了日期，添加WHERE条件过滤
        if date is None:
            # 没有日期：查询所有数据
            where = ""
        else:
            # 有日期：使用参数化查询
            # %s是参数占位符，实际值在执行时传入
            # 这样可以防止SQL注入攻击
            # 不要这样做：f" WHERE `date` = '{date}'" （容易被注入）
            where = f" WHERE `date` = %s"

        # ==================== 步骤5: 构建ORDER BY子句 ====================
        # 根据配置添加排序规则
        order_by = ""
        if web_module_data.order_by is not None:
            # 配置中有排序字段：添加到SQL
            # 示例：ORDER BY `code` ASC
            order_by = f" ORDER BY {web_module_data.order_by}"

        # ==================== 步骤6: 构建额外列 ====================
        # 某些表需要额外的计算列
        order_columns = ""
        if web_module_data.order_columns is not None:
            # 配置中有额外列：添加到SELECT
            # 示例：SELECT *, (close-open)/open AS change_rate
            order_columns = f",{web_module_data.order_columns}"

        # ==================== 步骤7: 构建完整SQL ====================
        # 组合所有部分形成完整的SQL语句
        # 示例：SELECT * FROM `stock_zh_a_spot_em` WHERE `date` = %s ORDER BY `code`
        sql = f" SELECT *{order_columns} FROM `{web_module_data.table_name}`{where}{order_by}"
        
        # ==================== 步骤8: 执行数据库查询 ====================
        # 使用self.db.query执行查询
        # 如果有WHERE条件，传入date参数（参数化查询）
        # self.db是BaseHandler中初始化的数据库连接
        data = self.db.query(sql, date)

        # ==================== 步骤9: 返回JSON数据 ====================
        # 使用自定义的MyEncoder序列化数据
        # 这样可以正确处理bytes和date类型
        self.write(json.dumps(data, cls=MyEncoder))


# ==================== AG Grid页面处理器 ====================

class GetStockHtmlAgGridHandler(webBase.BaseHandler, ABC):
    """
    处理GET请求，生成使用AG Grid的股票数据页面
    
    URL示例：
    /stock_web_aggrid?table_name=stock_zh_a_spot_em
    
    与GetStockHtmlHandler的区别：
    - 使用AG Grid代替SpreadJS
    - 更现代化的UI
    - 更好的性能和用户体验
    """
    @gen.coroutine
    def get(self):
        """
        Tornado异步GET请求处理方法
        """
        
        # ==================== 步骤1: 获取请求参数 ====================
        name = self.get_argument("table_name", default=None, strip=False)
        
        # ==================== 步骤2: 获取Web模块配置 ====================
        try:
            web_module_data = sswmd.stock_web_module_data().get_data(name)
        except KeyError:
            # 如果表名不存在，返回错误信息
            error_msg = f"表名 '{name}' 不存在。"
            available_tables = list(sswmd.stock_web_module_data().data.keys())
            error_msg += f"\n\n可用的表名：\n" + "\n".join(available_tables[:10])
            self.set_status(404)
            self.write(f"<html><body><h2>错误：{error_msg}</h2></body></html>")
            return
        
        # ==================== 步骤3: 获取交易日期 ====================
        run_date, run_date_nph = trd.get_trade_date_last()
        
        # ==================== 步骤4: 确定显示的日期 ====================
        # 优先从URL参数中获取date，如果没有则使用默认日期
        date_param = self.get_argument("date", default=None, strip=False)
        
        if date_param:
            # 如果URL中有date参数，使用它（保留用户之前选择的日期）
            date_now_str = date_param
            logging.info(f"✅ [AG Grid] 从 URL获取date参数: {date_param}")
        elif web_module_data.is_realtime:
            date_now_str = run_date_nph.strftime("%Y-%m-%d")
            logging.info(f"⚠️  [AG Grid] URL无date参数，使用非停牌日期: {date_now_str}")
        else:
            date_now_str = run_date.strftime("%Y-%m-%d")
            logging.info(f"⚠️  [AG Grid] URL无date参数，使用最新交易日: {date_now_str}")
        
        # ==================== 步骤5: 渲染HTML模板 ====================
        self.render("stock_web_aggrid.html", 
                    web_module_data=web_module_data,
                    date_now=date_now_str,
                    leftMenu=webBase.GetLeftMenu(self.request.uri))
