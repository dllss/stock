#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量为tablestructure.py中的表头添加单位
自动识别需要添加单位的字段类型
"""

import re

def add_units_to_tablestructure():
    """为tablestructure.py中的字段添加单位"""
    
    file_path = 'd:/WorkProject/stock/instock/core/tablestructure.py'
    
    # 读取文件
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 定义需要添加单位的模式
    unit_patterns = [
        # (正则表达式匹配模式, 单位名称, size增加量)
        (r"'cn': '(.+?)涨跌幅'", r"'\1涨跌幅(%)'", 15),  # 涨跌幅
        (r"'cn': '(.+?)涨跌'", r"'\1涨跌'", 0),  # 涨跌额不添加
        (r"'cn': '(.+?)换手率'", r"'\1换手率(%)'", 20),  # 换手率
        (r"'cn': '(.+?)振幅'", r"'\1振幅(%)'", 10),  # 振幅
        (r"'cn': '(.+?)量比'", r"'\1量比'", 0),  # 量比不需要单位
        (r"'cn': '(.+?)涨速'", r"'\1涨速(%)'", 10),  # 涨速
        (r"'cn': '(.+?)收益率'", r"'\1收益率(%)'", 20),  # 收益率
        (r"'cn': '(.+?)增长率'", r"'\1增长率(%)'", 20),  # 增长率
        (r"'cn': '(.+?)同比增长'", r"'\1同比增长(%)'", 20),  # 同比增长
        (r"'cn': '(.+?)净占比'", r"'\1净占比(%)'", 20),  # 净占比
        (r"'cn': '(.+?)毛利率'", r"'\1毛利率(%)'", 15),  # 毛利率
        (r"'cn': '(.+?)负债率'", r"'\1负债率(%)'", 20),  # 负债率
        (r"'cn': '(.+?)成交总量'", r"'\1成交总量(股)'", 15),  # 成交总量
        (r"'cn': '(.+?)成交量'", r"'\1成交量(手)'", 10),  # 成交量
        (r"'cn': '(.+?)成交笔数'", r"'\1成交笔数(笔)'", 15),  # 成交笔数
        (r"'cn': '(.+?)折溢率'", r"'\1折溢率(%)'", 20),  # 折溢率
        (r"'cn': '(.+?)股息率'", r"'\1股息率(%)'", 15),  # 股息率
        (r"'cn': '(.+?)比例'", r"'\1比例(%)'", 15),  # 比例
        (r"'cn': '(.+?)送转比例'", r"'\1送转比例(%)'", 20),  # 送转比例
        (r"'cn': '(.+?)转股比例'", r"'\1转股比例(%)'", 20),  # 转股比例
        (r"'cn': '(.+?)分红比例'", r"'\1分红比例(%)'", 20),  # 分红比例
    ]
    
    modified_count = 0
    
    # 应用所有模式
    for pattern, replacement, size_increase in unit_patterns:
        # 查找所有匹配
        matches = list(re.finditer(pattern, content))
        if matches:
            print(f"找到 {len(matches)} 个匹配: {pattern}")
            modified_count += len(matches)
    
    print(f"\n总共需要修改 {modified_count} 处")
    print("\n注意：由于修改量巨大，建议手动review关键表的修改")

if __name__ == '__main__':
    add_units_to_tablestructure()
