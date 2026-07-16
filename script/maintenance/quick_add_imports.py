#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速批量修改剩余任务文件
"""

import os
import re
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]

FILES = [
    'cn_stock_fund_flow_industry_job.py',
    'cn_stock_fund_flow_concept_job.py', 
    'cn_stock_selection_job.py',
    'cn_stock_spot_job.py',
    'cn_etf_spot_job.py',
]

BASE = ROOT_DIR / 'instock' / 'job' / 'data_tasks'

for filename in FILES:
    filepath = os.path.join(BASE, filename)
    
    if not os.path.exists(filepath):
        print(f"❌ {filename} 不存在")
        continue
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 检查是否已导入
    if 'from instock.job.task_utils import check_and_skip_if_exists' in content:
        print(f"⚠️  {filename} 已导入，跳过")
        continue
    
    # 添加import
    if 'import instock.core.stockfetch as stf' in content:
        content = content.replace(
            'import instock.core.stockfetch as stf\n',
            'import instock.core.stockfetch as stf\nfrom instock.job.task_utils import check_and_skip_if_exists\n'
        )
        print(f"✅ {filename} 已添加import")
    else:
        print(f"❌ {filename} 未找到import位置")
        continue
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

print("\n完成!请手动添加检查逻辑到每个文件的try块开头。")
