#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量修改任务文件，添加数据检查逻辑
"""

import os
import re

# 需要修改的文件列表
FILES_TO_MODIFY = [
    'cn_stock_bonus_job.py',
    'cn_stock_blocktrade_job.py',
    'cn_stock_chip_race_end_job.py',
    'cn_stock_chip_race_open_job.py',
    'cn_stock_fund_flow_concept_job.py',
    'cn_stock_fund_flow_industry_job.py',
    'cn_stock_fund_flow_job.py',
    'cn_stock_limitup_reason_job.py',
    'cn_stock_selection_job.py',
    'cn_stock_spot_job.py',
    'cn_etf_spot_job.py',
]

BASE_DIR = os.path.join(os.path.dirname(__file__), 'instock', 'job', 'data_tasks')


def add_import(filepath):
    """添加工具函数导入"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 检查是否已经导入
    if 'from instock.job.task_utils import check_and_skip_if_exists' in content:
        print(f"  ⚠️ 已存在导入，跳过")
        return False
    
    # 在最后一个import后添加
    pattern = r'(import instock\.core\.stockfetch as stf\n)'
    replacement = r'\1from instock.job.task_utils import check_and_skip_if_exists\n'
    
    new_content = re.sub(pattern, replacement, content)
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"  ✅ 已添加导入")
        return True
    else:
        print(f"  ❌ 未找到匹配位置")
        return False


def add_check_logic(filepath):
    """添加数据检查逻辑"""
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    modified = False
    new_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # 查找 "try:" 后面的第一行
        if line.strip() == 'try:' and i + 1 < len(lines):
            new_lines.append(line)
            i += 1
            
            # 检查下一行是否是 logging.info("")
            if i < len(lines) and 'logging.info("")' in lines[i]:
                # 在这之前插入检查逻辑
                # 先找到 table_name 的定义
                table_name_line = None
                for j in range(i, min(i + 20, len(lines))):
                    if "table_name = tbs.TABLE_" in lines[j] and "['name']" in lines[j]:
                        table_name_line = j
                        break
                
                if table_name_line:
                    # 提取表名变量
                    table_match = re.search(r"table_name = (tbs\.TABLE_\w+\['name'\])", lines[table_name_line])
                    if table_match:
                        table_expr = table_match.group(1)
                        
                        # 插入检查逻辑
                        indent = '        '
                        new_lines.append(f'{indent}table_name = {table_expr}\n')
                        new_lines.append(f'{indent}\n')
                        new_lines.append(f'{indent}# 步骤1: 检查当天是否已有数据\n')
                        new_lines.append(f'{indent}if check_and_skip_if_exists(table_name, date):\n')
                        new_lines.append(f'{indent}    return\n')
                        new_lines.append(f'{indent}\n')
                        new_lines.append(f'{indent}# 步骤2: 抓取数据\n')
                        
                        # 跳过原来的 table_name 定义行
                        skip_to = table_name_line + 1
                        while i < skip_to:
                            i += 1
                        
                        modified = True
                        print(f"  ✅ 已添加检查逻辑")
                        continue
        
        new_lines.append(line)
        i += 1
    
    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
    
    return modified


def main():
    print("=" * 60)
    print("批量修改任务文件 - 添加数据检查逻辑")
    print("=" * 60)
    print()
    
    success_count = 0
    skip_count = 0
    fail_count = 0
    
    for filename in FILES_TO_MODIFY:
        filepath = os.path.join(BASE_DIR, filename)
        
        if not os.path.exists(filepath):
            print(f"❌ {filename}: 文件不存在")
            fail_count += 1
            continue
        
        print(f"📝 处理 {filename}...")
        
        # 步骤1: 添加导入
        if add_import(filepath):
            success_count += 1
        
        # 步骤2: 添加检查逻辑（暂时跳过，需要手动处理）
        # if add_check_logic(filepath):
        #     success_count += 1
    
    print()
    print("=" * 60)
    print(f"完成! 成功: {success_count}, 跳过: {skip_count}, 失败: {fail_count}")
    print("=" * 60)


if __name__ == '__main__':
    main()
