#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
反爬虫延迟配置管理器
支持运行时动态读取配置文件,修改后立即生效
"""

import os
import json
import time

# 配置文件路径
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'delay_config.json')

# 默认配置
DEFAULT_CONFIG = {
    "DELAY_MIN": 9,
    "DELAY_MAX": 15,
    "RETRY_DELAY_MIN": 5,
    "RETRY_DELAY_MAX": 8,
    "SPECIAL_REQUEST_DELAY_MIN": 12,
    "SPECIAL_REQUEST_DELAY_MAX": 18
}


def get_delay_config():
    """
    获取延迟配置(每次调用都从文件读取,确保实时生效)
    
    Returns:
        dict: 配置字典
    """
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 合并默认配置,确保所有字段都存在
                for key, value in DEFAULT_CONFIG.items():
                    if key not in config:
                        config[key] = value
                return config
        else:
            # 如果配置文件不存在,创建默认配置
            save_delay_config(DEFAULT_CONFIG)
            return DEFAULT_CONFIG.copy()
    except Exception as e:
        print(f"⚠️ 读取配置文件失败: {e}, 使用默认配置")
        return DEFAULT_CONFIG.copy()


def save_delay_config(config):
    """
    保存延迟配置到文件
    
    Args:
        config: 配置字典
    """
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"❌ 保存配置文件失败: {e}")


def get_random_delay(delay_type='normal'):
    """
    获取随机延迟时间
    
    Args:
        delay_type: 延迟类型
            - 'normal': 正常请求延迟
            - 'retry': 重试延迟
            - 'special': 特殊请求延迟
    
    Returns:
        float: 延迟时间(秒)
    """
    import random
    
    config = get_delay_config()
    
    if delay_type == 'normal':
        return random.uniform(config['DELAY_MIN'], config['DELAY_MAX'])
    elif delay_type == 'retry':
        return random.uniform(config['RETRY_DELAY_MIN'], config['RETRY_DELAY_MAX'])
    elif delay_type == 'special':
        return random.uniform(config['SPECIAL_REQUEST_DELAY_MIN'], config['SPECIAL_REQUEST_DELAY_MAX'])
    else:
        raise ValueError(f"未知的延迟类型: {delay_type}")


def sleep_with_delay(delay_type='normal'):
    """
    休眠指定类型的延迟时间
    
    Args:
        delay_type: 延迟类型 ('normal', 'retry', 'special')
    
    Returns:
        float: 实际休眠时间
    """
    import time
    delay = get_random_delay(delay_type)
    time.sleep(delay)
    return delay


# ==================== 便捷函数(兼容旧代码) ====================

def get_DELAY_MIN():
    """获取最小延迟(实时读取)"""
    return get_delay_config()['DELAY_MIN']


def get_DELAY_MAX():
    """获取最大延迟(实时读取)"""
    return get_delay_config()['DELAY_MAX']


def get_RETRY_DELAY_MIN():
    """获取重试最小延迟(实时读取)"""
    return get_delay_config()['RETRY_DELAY_MIN']


def get_RETRY_DELAY_MAX():
    """获取重试最大延迟(实时读取)"""
    return get_delay_config()['RETRY_DELAY_MAX']


# ==================== 初始化默认配置文件 ====================
if not os.path.exists(CONFIG_FILE):
    save_delay_config(DEFAULT_CONFIG)
    print(f"✅ 已创建默认配置文件: {CONFIG_FILE}")
