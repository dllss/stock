#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K线可视化模块 - Bokeh交互式图表
==================================

功能说明：
本模块使用Bokeh库创建交互式的股票K线图表，包含：
1. 主K线图（蜡烛图）+ 均线
2. 筹码分布图（右侧）
3. 成交量柱状图
4. 技术指标图（MACD、KDJ等75种指标）
5. K线形态标注（61种形态识别）
6. 交互工具（缩放、平移、悬停、十字瞄准线等）

核心技术：
- Bokeh：Python交互式可视化库
- ColumnDataSource：Bokeh数据源，支持动态更新
- CustomJS：JavaScript回调，实现前端交互
- HoverTool：悬停提示工具
- CrosshairTool：十字瞄准线工具

图表组成：
┌─────────────────────────────────────────────┐
│  按钮区：关注 | 行情 | 资料 | 扫雷 | 形态     │
├──────────────────────────┬──────────────────┤
│   主K线图 + 均线          │  筹码分布图       │
│   (1000x300)             │  (160x300)       │
├──────────────────────────┴──────────────────┤
│   成交量图 (1000x120)                        │
├─────────────────────────────────────────────┤
│   技术指标Tab页 (MACD/KDJ/BOLL等)            │
└─────────────────────────────────────────────┘

交互功能：
1. 鼠标悬停：显示OHLCV详细信息 + 触发筹码分布计算
2. 滚轮缩放：放大/缩小K线图
3. 拖拽平移：移动查看不同时间段
4. 框选 zoom：选择区域放大
5. 十字瞄准线：精确定位价格和时间
6. 形态复选框：显示/隐藏特定形态标注
7. 指标Tab切换：查看不同技术指标

数据流程：
1. 获取股票历史数据
2. 计算技术指标（75种）
3. 识别K线形态（61种）
4. 准备筹码分布数据
5. 创建Bokeh图表对象
6. 添加各种图层（K线、均线、形态等）
7. 配置交互工具
8. 生成HTML组件（script + div）

依赖关系：
- bokeh：可视化库（需要3.8.1版本）
- numpy：数值计算
- instock.core.indicator.calculate_indicator：指标计算
- instock.core.pattern.pattern_recognitions：形态识别
- instock.core.kline.cyq：筹码分布计算
- instock.core.kline.cyq.js：JavaScript筹码分布算法

使用场景：
- Web端展示股票K线图
- 技术分析和形态识别
- 指标对比和回测
- 投资决策辅助

注意事项：
1. Bokeh版本需要匹配（3.8.1）
2. cyq.js文件必须存在且路径正确
3. 数据量较大时注意性能（默认360天）
4. JavaScript回调需要浏览器支持
5. 图表渲染需要较长时间，建议异步加载

性能优化：
1. 限制数据范围（threshold=120根K线）
2. 使用ColumnDataSource统一管理数据
3. 懒加载形态标注（默认隐藏）
4. Tab页切换指标（避免同时渲染所有指标）

输出格式：
返回字典：{"script": JavaScript代码, "div": HTML容器}
前端使用时：
```html
{{ raw script }}
{{ raw div }}
```
"""

import numpy as np
import json
import logging
import os.path
# 首映 bokeh 画图。
from bokeh import events
from bokeh.io import curdoc
from bokeh.transform import factor_cmap
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.palettes import Spectral11
from bokeh.layouts import column, row, layout
from bokeh.models import ColumnDataSource, HoverTool, CheckboxGroup, LabelSet, Button, CustomJS, \
    CDSView, BooleanFilter, TabPanel, Tabs, Div, Styles, CrosshairTool, Span, BoxSelectTool, WheelZoomTool, PanTool, \
    BoxZoomTool, ZoomInTool, ZoomOutTool, RedoTool, ResetTool, SaveTool, UndoTool, Text
import instock.core.tablestructure as tbs
import instock.core.indicator.calculate_indicator as idr
import instock.core.pattern.pattern_recognitions as kpr
import instock.core.kline.indicator_web_dic as iwd

__author__ = 'myh '
__date__ = '2023/4/6 '


# ==================== K线图表生成函数 ====================

"""
get_plot_kline - 生成股票K线交互式图表

功能：
根据股票代码和日期，生成包含K线、指标、形态、筹码分布的完整交互式图表

参数：
code (str): 股票代码
- 6位数字代码
- 例如："000001"（平安银行）、"600519"（贵州茅台）
- ETF代码以1或5开头

stock (DataFrame): 股票历史数据
- 必须包含：date, open, close, high, low, volume等字段
- 至少需要threshold + cyq_days天的数据（约330天）

date (str): 查询日期
- 格式：YYYY-MM-DD
- 例如："2024-01-01"

stock_name (str): 股票名称
- 用于显示在图表标题中
- 例如："平安银行"

返回：
dict: {"script": JavaScript代码, "div": HTML容器}
- script：包含所有交互逻辑的JavaScript代码
- div：包含图表结构的HTML容器
- 前端通过{{ raw script }}和{{ raw div }}渲染

如果出错返回None

执行流程：
1. 计算技术指标（75种，360天阈值）
2. 识别K线形态（61种，120天阈值）
3. 准备筹码分布数据（210天）
4. 创建K线图主体
5. 添加均线和K线蜡烛
6. 配置悬停工具和十字瞄准线
7. 创建筹码分布图（右侧）
8. 添加形态标注（LabelSet）
9. 创建成交量图
10. 创建形态复选框
11. 创建指标Tab页（31种指标）
12. 创建按钮和链接
13. 组合所有组件
14. 生成script和div

图表结构详解：

A. 主K线图（p_kline）
   - 尺寸：1000x300像素
   - X轴：K线索引（0到k_length）
   - Y轴：价格范围（min_price到max_price）
   - 内容：
     * K线蜡烛（红涨绿跌）
     * 5条均线（close, ma10, ma20, ma50, ma200）
     * 形态标注（上涨红色，下跌绿色）
   - 工具：平移、框选、缩放、滚轮、撤销、重做、重置、保存

B. 筹码分布图（p_cyq）
   - 尺寸：160x300像素（右侧）
   - Y轴：与K线图同步
   - 内容：
     * 获利筹码（红色区域）
     * 套牢筹码（蓝色区域）
     * 平均成本线（红色虚线）
   - 交互：鼠标悬停K线时动态计算并更新

C. 成交量图（p_volume）
   - 尺寸：1000x120像素
   - X轴：与K线图同步
   - 内容：
     * 成交量柱（红涨绿跌）
     * 5日均量线
     * 10日均量线

D. 指标Tab页（tabs_indicators）
   - 宽度：1000像素
   - 每个Tab一个指标图
   - 31种指标：MACD, KDJ, BOLL, RSI等
   - 每个指标下方有详细说明

E. 控制区
   - 按钮：关注/取关、行情链接、资料链接、扫雷评级、形态说明
   - 复选框：形态显示/隐藏（全选/全弃）
   - 布局：垂直排列在左侧

关键技术点：

1. 颜色映射（factor_cmap）
   ```python
   c_cmap = factor_cmap("is_red", ["red", "green"], ["1", "0"])
   ```
   - 根据is_red字段自动选择颜色
   - "1" → red（上涨）
   - "0" → green（下跌）

2. 数据源（ColumnDataSource）
   ```python
   source = ColumnDataSource(data)
   ```
   - 统一管理所有图表数据
   - 支持动态更新
   - 多个图表可以共享同一数据源

3. JavaScript回调（CustomJS）
   ```python
   cqy_callback = CustomJS.from_file("cyq.js", ...)
   ```
   - 从外部JS文件加载代码
   - 传递Python变量给JS
   - 实现前端交互逻辑

4. 形态标注（LabelSet）
   ```python
   LabelSet(x='index', y='high', text='label_cn', ...)
   ```
   - 在K线图上标注形态名称
   - 上涨形态：上方红色文字
   - 下跌形态：下方绿色文字
   - 可通过复选框控制显示

5. 指标分层显示（CDSView + BooleanFilter）
   ```python
   view_upper = CDSView(filter=BooleanFilter(up))
   p_indicator.vbar(..., view=view_upper)
   ```
   - MACD柱状图分上下两部分
   - 正值绿色，负值红色
   - 使用不同的View过滤数据

使用示例：
```python
# 在Web Handler中调用
code = "000001"
date = "2024-01-01"
stock = fetch_stock_hist((date, code))
result = get_plot_kline(code, stock, date, "平安银行")

# 在Tornado模板中使用
self.render("stock_indicators.html", 
            script=result["script"], 
            div=result["div"])
```

注意事项：
1. 股票代码以1或5开头的是ETF，处理方式不同
2. 需要足够的历史数据（至少330天）
3. cyq.js文件必须在同一目录
4. Bokeh版本需要匹配
5. 大量数据时渲染较慢，建议限制时间范围

常见错误：
- 数据不足：返回None
- 代码错误：抛出异常
- JS文件缺失：筹码分布不工作
- 版本不匹配：图表渲染失败
"""
def get_plot_kline(code, stock, date, stock_name):
    """
    生成股票K线交互式图表
    
    参数：
    code (str): 股票代码
    stock (DataFrame): 股票历史数据
    date (str): 查询日期
    stock_name (str): 股票名称
    
    返回：
    dict: {"script": str, "div": str} 或 None
    """
    
    plot_list = []  # 结果列表（未使用，保留兼容性）
    
    try:
        # ==================== 步骤1: 计算技术指标 ====================
        # 获取股票的32种技术指标
        # threshold=360：需要至少360天的数据来计算长期均线（ma200等）
        data = idr.get_indicators(stock, date, threshold=360)
        
        # 检查是否成功获取指标
        if data is None:
            return None
        
        # ==================== 步骤2: 识别K线形态 ====================
        # 设置形态识别的时间窗口
        threshold = 120  # 只识别最近120根K线的形态
        
        # 获取形态数据的列定义
        stock_column = tbs.STOCK_KLINE_PATTERN_DATA['columns']
        
        # 识别61种K线形态
        # 返回的data中会新增形态列（正值表示看涨形态，负值表示看跌形态）
        data = kpr.get_pattern_recognitions(data, stock_column, threshold=threshold)
        
        # 检查是否成功识别形态
        if data is None:
            return None
        
        # ==================== 步骤3: 准备筹码分布数据 ====================
        # 筹码分布需要更多的历史数据
        cyq_days = 210  # 使用210天的数据计算筹码分布
        
        # 截取所需的数据范围：threshold + cyq_days = 120 + 210 = 330天
        # copy()创建副本，避免修改原始数据
        cyq_stock = stock.tail(n=threshold + cyq_days).copy()
        
        # ==================== 步骤4: 计算价格范围和准备数据 ====================
        # 计算Y轴的价格范围（留2%的边距）
        min_price = data['low'].min() * 0.98   # 最低价向下扩展2%
        max_price = data['high'].max() * 1.02  # 最高价向上扩展2%
        
        # 获取K线数量
        k_length = len(data.index)
        
        # 添加索引列（用于X轴）
        # np.arange(k_length)生成[0, 1, 2, ..., k_length-1]
        data['index'] = list(np.arange(k_length))
        
        # 添加颜色标识列
        # 收盘价 > 开盘价 → "1"（红色，上涨）
        # 收盘价 <= 开盘价 → "0"（绿色，下跌）
        # apply()对每一行应用lambda函数
        data['is_red'] = data.apply(lambda row: "1" if row['close'] > row['open'] else "0", axis=1)
        
        # ==================== 步骤5: 创建颜色映射和数据源 ====================
        # 颜色，红盘或绿盘
        # factor_cmap根据字段值映射颜色
        # "is_red"字段："1"→red, "0"→green
        c_cmap = factor_cmap("is_red", ["red", "green"], ["1", "0"])
        
        # K线图数据源
        # ColumnDataSource是Bokeh的核心数据容器
        # 所有图表元素都从这个数据源读取数据
        source = ColumnDataSource(data)
        
        # ==================== 步骤6: 创建交互工具 ====================
        # 工具条：定义用户可用的交互工具
        # 每个工具都有description，鼠标悬停时显示提示
        tools = pan, box_select, box_zoom, wheel_zoom, zoom_in, zoom_out, undo, redo, reset, save = \
            PanTool(description="平移"), \
            BoxSelectTool(description="方框选取"), \
            BoxZoomTool(description="方框缩放"), \
            WheelZoomTool(description="滚轮缩放"), \
            ZoomInTool(description="放大"), \
            ZoomOutTool(description="缩小"), \
            UndoTool(description="撤销"), \
            RedoTool(description="重做"), \
            ResetTool(description="重置"), \
            SaveTool(description="保存", filename=f"InStock_{code}({date})")
        
        # ==================== 步骤7: 创建主K线图 ====================
        # figure()创建Bokeh图表对象
        # width=1000, height=300：图表尺寸
        # x_range=(0, k_length + 1)：X轴范围（K线索引）
        # y_range=(min_price, max_price)：Y轴范围（价格）
        # min_border_left=80：左边距（容纳Y轴标签）
        # tools=tools：添加之前定义的工具
        # toolbar_location='above'：工具栏位置（顶部）
        p_kline = figure(
            width=1000, 
            height=300, 
            x_range=(0, k_length + 1), 
            y_range=(min_price, max_price), 
            min_border_left=80,
            tools=tools, 
            toolbar_location='above'
        )
        
        # ==================== 步骤8: 添加均线 ====================
        # 均线标签和颜色
        # sam_labels：要显示的均线名称
        # Spectral11：Bokeh提供的调色板（11种颜色）
        sam_labels = ("close", "ma10", "ma20", "ma50", "ma200")
        
        # 遍历每条均线，添加到图表
        for name, color in zip(sam_labels, Spectral11):
            # line()绘制折线
            # x='index'：X轴使用索引列
            # y=name：Y轴使用对应的均价值
            # source=source：从数据源读取
            # legend_label：图例标签（中文名称）
            # color：线条颜色
            # line_width=1.5：线宽
            # alpha=0.8：透明度（0-1之间）
            p_kline.line(
                x='index', 
                y=name, 
                source=source, 
                legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                color=color, 
                line_width=1.5, 
                alpha=0.8
            )
        
        # 图例位置：左上角
        p_kline.legend.location = "top_left"
        
        # 图例点击策略：hide（点击隐藏/显示对应的线）
        p_kline.legend.click_policy = "hide"
        
        # ==================== 步骤9: 添加K线蜡烛 ====================
        # segment()绘制股价的高低线（影线）
        # x0='index', y0='high'：起点（索引，最高价）
        # x1='index', y1='low'：终点（索引，最低价）
        # color=c_cmap：使用颜色映射（红涨绿跌）
        c_segment = p_kline.segment(
            x0='index', 
            y0='high', 
            x1='index', 
            y1='low', 
            color=c_cmap, 
            source=source
        )
        
        # vbar()绘制股价的实体柱
        # 'index'：X轴位置
        # 0.5：柱宽度
        # 'open'：柱底部（开盘价）
        # 'close'：柱顶部（收盘价）
        # fill_color=c_cmap：填充颜色
        # line_color=c_cmap：边框颜色
        # hover_fill_alpha=0.5：悬停时透明度
        p_kline.vbar(
            'index', 
            0.5, 
            'open', 
            'close', 
            fill_color=c_cmap, 
            line_color=c_cmap, 
            source=source,
            hover_fill_alpha=0.5
        )
        
        # ==================== 步骤10: 配置悬停提示 ====================
        # tooltips：悬停时显示的信息
        # @字段名：引用数据源中的列
        # {¥0}：金额格式化（人民币符号，0位小数）
        # {%}：百分比格式化
        tooltips = [
            ('日期', '@date'), 
            ('开盘', '@open'),
            ('最高', '@high'), 
            ('最低', '@low'),
            ('收盘', '@close'), 
            ('涨跌', '@quote_change%'),
            ('金额', '@amount{¥0}'), 
            ('换手', '@turnover%')
        ]
        
        # HoverTool：悬停工具
        # tooltips：显示的信息模板
        # description：工具描述
        # renderers=[c_segment]：只在悬停在K线段上时显示
        hover = HoverTool(tooltips=tooltips, description="悬停", renderers=[c_segment])
        
        # ==================== 步骤11: 添加十字瞄准线 ====================
        # CrosshairTool：十字瞄准线工具
        # overlay：覆盖层样式
        # Span(dimension="width")：水平线（虚线）
        # Span(dimension="height")：垂直线（点线）
        crosshair = CrosshairTool(
            overlay=[
                Span(dimension="width", line_dash="dashed", line_width=2),
                Span(dimension="height", line_dash="dotted", line_width=2)
            ],
            description="十字瞄准线"
        )
        
        # ==================== 步骤12: 创建筹码分布图 ====================
        # Div：HTML容器，用于显示筹码分布的文字信息
        div_cyq = Div()
        
        # figure()创建筹码分布图
        # width=160：宽度较窄（右侧小图）
        # height=p_kline.height：高度与K线图一致
        # y_range=p_kline.y_range：Y轴范围与K线图同步
        # min_border_left=0：不需要左边距
        # toolbar_location=None：不显示工具栏
        # y_axis_location="right"：Y轴在右侧
        p_cyq = figure(
            width=160, 
            height=p_kline.height, 
            y_range=p_kline.y_range, 
            min_border_left=0,
            toolbar_location=None, 
            y_axis_location="right"
        )
        
        # 隐藏X网格线
        p_cyq.xgrid.grid_line_color = None
        
        # 隐藏X轴
        p_cyq.xaxis.visible = False
        
        # 添加平均成本线（红色虚线）
        cyq_avgcost_line = p_cyq.line(
            x="x", 
            y="y", 
            color="red", 
            line_width=2, 
            line_dash="dotted"
        )
        
        # 添加平均成本文字标签
        # Text()创建文字glyph
        # text_align="center"：文字居中对齐
        cyq_avgcost_text = p_cyq.add_glyph(
            ColumnDataSource(dict(x=[], y=[], text=[])),
            glyph=Text(x="x", y="y", text="text", text_align="center")
        )
        
        # 添加获利筹码区域（红色）
        # varea()填充面积图
        # y1="y1"：上边界
        # y2=0：下边界（X轴）
        # fill_alpha=0.3：透明度30%
        # fill_color="red"：红色表示获利盘
        cyq_down_varea = p_cyq.varea(
            x="x", 
            y1="y1", 
            y2=0, 
            fill_alpha=0.3, 
            fill_color="red"
        )
        
        # 添加套牢筹码区域（蓝色）
        cyq_up_varea = p_cyq.varea(
            x="x", 
            y1="y1", 
            y2=0, 
            fill_alpha=0.3, 
            fill_color="blue"
        )
        
        # ==================== 步骤13: 准备筹码分布JavaScript回调 ====================
        # 将筹码分布数据转换为JSON格式
        # to_json(orient="records")：转换为记录格式的JSON字符串
        json_str_stock = cyq_stock.to_json(orient="records")
        
        # 解析JSON并重新序列化（格式化）
        # indent=2：缩进2空格，便于阅读
        js_array_str_stock = json.dumps(json.loads(json_str_stock), indent=2)
        
        # 创建JavaScript回调（非初始化）
        # CustomJS.from_file()从外部JS文件加载代码
        # isinit=False：不是初始化调用
        # 传递Python变量给JS：div_cyq, cyq_avgcost_line等
        cqy_callback = CustomJS.from_file(
            os.path.join(os.path.dirname(__file__), "cyq.js"),
            isinit=False,
            div_cyq=div_cyq,
            cyq_avgcost_line=cyq_avgcost_line.data_source,
            cyq_avgcost_text=cyq_avgcost_text.data_source,
            cyq_down_varea=cyq_down_varea.data_source,
            cyq_up_varea=cyq_up_varea.data_source,
            kline_data=js_array_str_stock,
            k_range=k_length,
            cyq_days=cyq_days
        )
        
        # 创建悬停时的筹码分布回调
        # tooltips=None：不显示悬停提示（避免干扰）
        # callback=cqy_callback：执行筹码分布计算
        # renderers=[c_segment]：只在悬停K线时触发
        cqy_hover = HoverTool(tooltips=None, callback=cqy_callback, renderers=[c_segment])
        
        # 创建初始化回调
        # isinit=True：页面加载时立即执行
        cqy_callback_isinit = CustomJS.from_file(
            os.path.join(os.path.dirname(__file__), "cyq.js"),
            isinit=True,
            div_cyq=div_cyq,
            cyq_avgcost_line=cyq_avgcost_line.data_source,
            cyq_avgcost_text=cyq_avgcost_text.data_source,
            cyq_down_varea=cyq_down_varea.data_source,
            cyq_up_varea=cyq_up_varea.data_source,
            kline_data=js_array_str_stock,
            k_range=k_length,
            cyq_days=cyq_days
        )
        
        # 注册文档就绪事件
        # 当页面加载完成时，执行初始化回调
        curdoc().on_event(events.DocumentReady, cqy_callback_isinit)
        
        # ==================== 步骤14: 添加工具到K线图 ====================
        # 将悬停工具、筹码分布工具、十字瞄准线添加到K线图
        p_kline.add_tools(hover, cqy_hover, crosshair)
        
        # ==================== 步骤15: 添加K线形态标注 ====================
        # 形态缺省是否显示
        pattern_is_show = True
        
        # 复选框参数和代码
        checkboxes_args = {}  # 传递给JavaScript的参数
        checkboxes_code = """let acts = cb_obj.active;"""  # JavaScript代码
        
        # 形态标签列表
        pattern_labels = []
        
        # 遍历所有形态类型
        i = 0
        for k in stock_column:
            # 获取形态的中文名称
            label_cn = stock_column[k]['cn']
            
            # --- 处理看涨形态（正值）---
            # 创建布尔掩码：筛选出该形态为正值（看涨）的K线
            label_mask_u = (data[k] > 0)
            
            # 复制符合条件的数据
            label_data_u = data.loc[label_mask_u].copy()
            
            # 标记是否有该形态
            isHas = False
            
            # 如果有看涨形态数据
            if len(label_data_u.index) > 0:
                # 添加形态名称列
                label_data_u.loc[:, 'label_cn'] = label_cn
                
                # 创建数据源
                label_source_u = ColumnDataSource(label_data_u)
                
                # 创建LabelSet（形态标注）
                # x='index'：X轴位置（K线索引）
                # y='high'：Y轴位置（最高价上方）
                # text="label_cn"：显示的文字
                # x_offset=7, y_offset=5：偏移量（避免遮挡K线）
                # angle=90：旋转90度（垂直文字）
                # text_color='red'：红色文字
                # text_font_style='bold'：粗体
                # text_font_size="9pt"：字体大小
                # visible=pattern_is_show：初始可见性
                locals()[f'pattern_labels_u_{str(i)}'] = LabelSet(
                    x='index', 
                    y='high', 
                    text="label_cn",
                    source=label_source_u, 
                    x_offset=7, 
                    y_offset=5,
                    angle=90, 
                    angle_units='deg', 
                    text_color='red',
                    text_font_style='bold', 
                    text_font_size="9pt",
                    visible=pattern_is_show
                )
                
                # 将标注添加到图表
                p_kline.add_layout(locals()[f'pattern_labels_u_{str(i)}'])
                
                # 添加到复选框参数
                checkboxes_args[f'lsu{str(i)}'] = locals()[f'pattern_labels_u_{str(i)}']
                
                # 生成JavaScript代码：控制可见性
                # acts.includes(i)：如果复选框被选中，则显示
                checkboxes_code = f"{checkboxes_code}lsu{i}.visible = acts.includes({i});"
                
                # 添加到标签列表
                pattern_labels.append(label_cn)
                isHas = True
            
            # --- 处理看跌形态（负值）---
            # 创建布尔掩码：筛选出该形态为负值（看跌）的K线
            label_mask_d = (data[k] < 0)
            
            # 复制符合条件的数据
            label_data_d = data.loc[label_mask_d].copy()
            
            # 如果有看跌形态数据
            if len(label_data_d.index) > 0:
                # 添加形态名称列
                label_data_d.loc[:, 'label_cn'] = label_cn
                
                # 创建数据源
                label_source_d = ColumnDataSource(label_data_d)
                
                # 创建LabelSet（形态标注）
                # y='low'：Y轴位置（最低价下方）
                # x_offset=-7, y_offset=-5：向左下偏移
                # angle=270：旋转270度（垂直文字，方向相反）
                # text_color='green'：绿色文字
                locals()[f'pattern_labels_d_{str(i)}'] = LabelSet(
                    x='index', 
                    y='low', 
                    text='label_cn',
                    source=label_source_d, 
                    x_offset=-7, 
                    y_offset=-5, 
                    angle=270, 
                    angle_units='deg',
                    text_color='green',
                    text_font_style='bold', 
                    text_font_size="9pt",
                    visible=pattern_is_show
                )
                
                # 将标注添加到图表
                p_kline.add_layout(locals()[f'pattern_labels_d_{str(i)}'])
                
                # 添加到复选框参数
                checkboxes_args[f'lsd{str(i)}'] = locals()[f'pattern_labels_d_{str(i)}']
                
                # 生成JavaScript代码
                checkboxes_code = f"{checkboxes_code}lsd{i}.visible = acts.includes({i});"
                
                # 如果之前没有添加过该形态，现在添加
                if not isHas:
                    pattern_labels.append(label_cn)
                    isHas = True
            
            # 如果有该形态的标注，计数器加1
            if isHas:
                i += 1
        
        # 隐藏K线图的X轴（成交量图会显示日期）
        p_kline.xaxis.visible = False
        
        # 设置底部边距为0（与成交量图无缝连接）
        p_kline.min_border_bottom = 0
        
        # ==================== 步骤16: 创建成交量图 ====================
        # figure()创建成交量图
        # width=p_kline.width：宽度与K线图一致
        # height=120：高度较矮
        # x_range=p_kline.x_range：X轴范围与K线图同步
        # min_border_left=p_kline.min_border_left：左边距一致
        # tools=tools：使用相同的工具
        # toolbar_location=None：不显示工具栏（与K线图共用）
        p_volume = figure(
            width=p_kline.width, 
            height=120, 
            x_range=p_kline.x_range,
            min_border_left=p_kline.min_border_left, 
            tools=tools, 
            toolbar_location=None
        )
        
        # 添加均量线
        vol_labels = ("vol_5", "vol_10")  # 5日均量和10日均量
        for name, color in zip(vol_labels, Spectral11):
            # line()绘制均量线
            p_volume.line(
                x=data['index'], 
                y=data[name], 
                legend_label=name, 
                color=color, 
                line_width=1.5, 
                alpha=0.8
            )
        
        # 图例位置和点击策略
        p_volume.legend.location = "top_left"
        p_volume.legend.click_policy = "hide"
        
        # 添加成交量柱
        # vbar()绘制柱状图
        # 'index'：X轴位置
        # 0.5：柱宽度
        # 0：柱底部（X轴）
        # 'volume'：柱高度（成交量）
        # color=c_cmap：颜色（红涨绿跌）
        p_volume.vbar('index', 0.5, 0, 'volume', color=c_cmap, source=source)
        
        # 添加十字瞄准线
        p_volume.add_tools(crosshair)
        
        # 自定义X轴标签：显示日期而不是索引
        # enumerate(data['date'])：生成(索引, 日期)对
        # {i: date for ...}：创建字典映射
        p_volume.xaxis.major_label_overrides = {
            i: date for i, date in enumerate(data['date'])
        }
        # p_volume.xaxis.major_label_orientation = pi / 4  # 可选：旋转标签
        
        # ==================== 步骤17: 创建形态复选框 ====================
        # CheckboxGroup：复选框组件
        # labels=pattern_labels：所有形态名称
        # active=list(range(len(pattern_labels)))：默认全部选中
        # 如果pattern_is_show=False，则active=[]（全部不选中）
        pattern_checkboxes = CheckboxGroup(
            labels=pattern_labels,
            active=list(range(len(pattern_labels))) if pattern_is_show else []
        )
        
        # 设置复选框高度（与K线图+成交量图总高度一致）
        # pattern_checkboxes.inline = True  # 已废弃的属性
        pattern_checkboxes.height = p_kline.height + p_volume.height
        
        # 如果有形态标注，添加JavaScript回调
        if checkboxes_args:
            # js_on_change()：当复选框状态改变时执行
            # 'active'：监听active属性变化
            # CustomJS()：创建JavaScript回调
            # args=checkboxes_args：传递参数给JS
            # code=checkboxes_code：JS代码（控制各个标注的可见性）
            pattern_checkboxes.js_on_change(
                'active', 
                CustomJS(args=checkboxes_args, code=checkboxes_code)
            )
        
        # 将复选框放入column布局
        ck = column(row(pattern_checkboxes))
        
        # ==================== 步骤18: 创建全选/全弃按钮 ====================
        # Button()创建按钮
        select_all = Button(label="全选")
        select_none = Button(label='全弃')
        
        # 全选按钮：点击时选中所有复选框
        # js_on_event()：监听按钮点击事件
        # "button_click"：点击事件
        # CustomJS()：JavaScript回调
        # args={'pcs': pattern_checkboxes, 'pls': pattern_labels}：传递参数
        # code="pcs.active = Array.from(pls, (x, i) => i);"：将所有索引设为active
        select_all.js_on_event(
            "button_click", 
            CustomJS(
                args={'pcs': pattern_checkboxes, 'pls': pattern_labels},
                code="pcs.active = Array.from(pls, (x, i) => i);"
            )
        )
        
        # 全弃按钮：点击时取消所有选中
        select_none.js_on_event(
            "button_click", 
            CustomJS(
                args={'pcs': pattern_checkboxes},
                code="pcs.active = [];"
            )
        )
        
        # ==================== 步骤19: 创建指标Tab页 ====================
        # tabs列表：存储所有Tab面板
        tabs = []
        
        # 遍历所有指标配置
        for conf in iwd.indicators_dic:
            # 创建指标图
            # width=p_kline.width：宽度与K线图一致
            # height=150：高度适中
            # x_range=p_kline.x_range：X轴同步
            p_indicator = figure(
                width=p_kline.width, 
                height=150, 
                x_range=p_kline.x_range,
                min_border_left=p_kline.min_border_left, 
                tools=tools, 
                toolbar_location=None
            )
            
            # 遍历指标中的所有字段
            for name, color in zip(conf["dic"], Spectral11):
                # 特殊处理：MACD和PPO的柱状图
                if name == 'macdh' or name == 'ppoh':
                    # 判断值是正还是负
                    up = [True if val > 0 else False for val in source.data[name]]
                    down = [True if val < 0 else False for val in source.data[name]]
                    
                    # 创建视图过滤器
                    view_upper = CDSView(filter=BooleanFilter(up))   # 正值视图
                    view_lower = CDSView(filter=BooleanFilter(down)) # 负值视图
                    
                    # 绘制负值柱（绿色，在X轴下方）
                    p_indicator.vbar(
                        'index', 0.1, 0, name,
                        legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                        color='green', 
                        source=source, 
                        view=view_lower
                    )
                    
                    # 绘制正值柱（红色，在X轴上方）
                    p_indicator.vbar(
                        'index', 0.1, name, 0,
                        legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                        color='red', 
                        source=source, 
                        view=view_upper
                    )
                else:
                    # 普通指标：绘制折线
                    p_indicator.line(
                        x='index', 
                        y=name,
                        legend_label=tbs.get_field_cn(name, tbs.STOCK_STATS_DATA),
                        color=color, 
                        source=source, 
                        line_width=1.5, 
                        alpha=0.8
                    )
            
            # 图例位置和点击策略
            p_indicator.legend.location = "top_left"
            p_indicator.legend.click_policy = "hide"
            
            # 添加十字瞄准线
            p_indicator.add_tools(crosshair)
            
            # 隐藏X轴
            p_indicator.xaxis.visible = False
            p_indicator.min_border_bottom = 0
            
            # 创建指标说明文字
            # Div()创建HTML容器
            # text=f"""..."""：包含指标的详细说明链接
            div_indicator = Div(
                text=f"""★★★★★指标详细解读：{conf["desc"]}""", 
                width=p_kline.width
            )
            
            # 创建Tab面板
            # TabPanel()：单个Tab
            # child=column(p_indicator, row(div_indicator))：子组件（指标图 + 说明文字）
            # title=conf["title"]：Tab标题（如"MACD"）
            tabs.append(
                TabPanel(
                    child=column(p_indicator, row(div_indicator)), 
                    title=conf["title"]
                )
            )
        
        # 创建Tabs组件（包含所有Tab）
        # tabs=tabs：所有Tab面板
        # tabs_location='below'：Tab标签在下方
        # width=p_kline.width：宽度与K线图一致
        # stylesheets：自定义样式
        tabs_indicators = Tabs(
            tabs=tabs, 
            tabs_location='below', 
            width=p_kline.width, 
            stylesheets=[
                {
                    '.bk-tab': Styles(padding='1px 1.4px', font_size='xx-small'),
                    '.bk-tab.bk-active': Styles(background_color='yellow', color='red')
                }
            ]
        )
        
        # ==================== 步骤20: 创建关注和链接按钮 ====================
        # 判断是否为ETF（代码以1或5开头）
        if code.startswith(('1', '5')):
            # ETF不显示关注按钮
            div_attention = Div()
        else:
            # 导入数据库模块
            import instock.lib.database as mdb
            
            # 获取关注表名
            table_name = tbs.TABLE_CN_STOCK_ATTENTION['name']
            
            # 构造SQL查询：检查该股票是否已关注
            # EXISTS()：存在性检查（比COUNT更快）
            _sql = f"SELECT EXISTS(SELECT 1 FROM `{table_name}` WHERE `code` = '{code}')"
            
            try:
                # 执行查询，返回0或1
                rc = mdb.executeSqlCount(_sql)
            except Exception as e:
                # 出错时默认为0（未关注）
                rc = 0
            
            # 根据查询结果设置按钮状态
            if rc == 0:
                cvalue = "0"  # 未关注
                cname = "关注"
            else:
                cvalue = "1"  # 已关注
                cname = "取关"
            
            # 创建关注按钮
            # onclick="attention('{code}',this)"：点击时调用JavaScript函数
            # return false：阻止默认行为
            div_attention = Div(
                text=f"""<button id="attentionId" value="{cvalue}" onclick="attention('{code}',this);return false;">{cname}</button>""",
                width=47
            )
        
        # 东方财富股票行情链接
        # 根据代码前缀判断市场：6开头是沪市（SH），其他是深市（SZ）
        if code.startswith("6"):
            code_name = f"SH{code}"
        else:
            code_name = f"SZ{code}"
        
        # 创建行情链接
        div_dfcf_hq = Div(
            text=f"""<a href="https://quote.eastmoney.com/{code_name}.html" target="_blank">{code}{stock_name}行情</a>""",
            width=150
        )
        
        # 资料链接（ETF不显示）
        if code.startswith(('1', '5')):
            div_dfcf_zl = Div()
        else:
            div_dfcf_zl = Div(
                text=f"""<a href="https://emweb.eastmoney.com/PC_HSF10/OperationsRequired/Index?code={code_name}" target="_blank">资料</a>""",
                width=40
            )
        
        # 扫雷评级链接（ETF不显示）
        if code.startswith(('1', '5')):
            div_dfcf_pj = Div()
        else:
            div_dfcf_pj = Div(
                text=f"""<a href="http://page1.tdx.com.cn:7615/site/pcwebcall_static/bxb/bxb.html?code={code}&color=0" target="_blank">扫雷评级</a>""",
                width=80
            )
        
        # 形态说明链接
        div_dfcf_pr = Div(
            text=f"""<a href="https://www.ljjyy.com/archives/2023/04/100718.html" target="_blank">形态</a>""",
            width=40
        )
        
        # ==================== 步骤21: 组合所有组件 ====================
        # layout()创建整体布局
        # row()：水平排列
        # column()：垂直排列
        
        # 布局结构：
        # ┌─────────────────────────────────────────┐
        # │ row(按钮1, 按钮2, 按钮3, ...)           │
        # ├─────────────────────────────────────────┤
        # │ column(                                 │
        # │   row(K线图, 筹码图),                   │
        # │   row(column(成交量, 指标), 筹码文字)   │
        # │ )                                       │
        # ├─────────────────────────────────────────┤
        # │ column(形态复选框)                      │
        # └─────────────────────────────────────────┘
        
        layouts = layout(
            row(
                column(
                    # 第一行：按钮区
                    row(
                        children=[
                            div_attention,      # 关注按钮
                            div_dfcf_hq,        # 行情链接
                            div_dfcf_zl,        # 资料链接
                            div_dfcf_pj,        # 扫雷评级
                            div_dfcf_pr,        # 形态说明
                            select_all,         # 全选按钮
                            select_none         # 全弃按钮
                        ],
                        align='end'  # 底部对齐
                    ),
                    # 第二行：K线图 + 筹码图
                    row(children=[p_kline, p_cyq]),
                    # 第三行：成交量 + 指标 + 筹码文字
                    row(children=[column(p_volume, tabs_indicators), div_cyq])
                ),
                # 左侧：形态复选框
                ck
            )
        )
        
        # ==================== 步骤22: 生成HTML组件 ====================
        # components()将Bokeh图表转换为HTML
        # script：JavaScript代码（包含所有交互逻辑）
        # div：HTML容器（包含图表结构）
        script, div = components(layouts)
        
        # 返回结果
        return {"script": script, "div": div}
    
    except Exception as e:
        # 捕获所有异常，记录日志
        logging.error(f"visualization.get_plot_kline处理异常：{e}")
    
    # 出错时返回None
    return None
