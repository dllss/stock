#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
文件缓存工具
提供通用的文件缓存装饰器和缓存管理功能
"""
import json
import os
import functools
from pathlib import Path
from datetime import datetime, timedelta
from typing import Callable, Any, Optional

__author__ = 'myh '
__date__ = '2026/02/24 '


class FileCache:
    """
    文件缓存管理器
    """
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        初始化缓存管理器
        :param cache_dir: 缓存目录，默认为 instock/cache/
        """
        if cache_dir is None:
            base_dir = os.path.dirname(os.path.dirname(__file__))
            cache_dir = os.path.join(base_dir, 'cache')
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_cache_path(self, cache_name: str) -> Path:
        """
        获取缓存文件路径
        :param cache_name: 缓存名称
        :return: 缓存文件路径
        """
        return self.cache_dir / f"{cache_name}.json"
    
    def get_meta_path(self, cache_name: str) -> Path:
        """
        获取缓存元数据文件路径
        :param cache_name: 缓存名称
        :return: 元数据文件路径
        """
        return self.cache_dir / f"{cache_name}.meta.json"
    
    def is_cache_valid(self, cache_name: str, hours: int = 24) -> bool:
        """
        检查缓存是否有效
        :param cache_name: 缓存名称
        :param hours: 缓存有效期（小时）
        :return: 是否有效
        """
        cache_file = self.get_cache_path(cache_name)
        meta_file = self.get_meta_path(cache_name)
        
        # 数据文件和元数据文件必须都存在
        if not cache_file.exists() or not meta_file.exists():
            return False
        
        try:
            # 读取元数据文件
            with open(meta_file, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
            
            # 使用元数据中的过期时间
            expires_at_str = meta_data['expires_at']
            expires_at = datetime.fromisoformat(expires_at_str)
            
            # 检查是否过期
            is_valid = datetime.now() < expires_at
            if not is_valid:
                print(f"⏰ 缓存已过期: {cache_name} (过期时间: {expires_at_str})")
            return is_valid
            
        except (json.JSONDecodeError, KeyError, ValueError, FileNotFoundError) as e:
            print(f"❌ 读取元数据失败: {e}")
            return False
    
    def read_cache(self, cache_name: str) -> Any:
        """
        读取缓存数据
        :param cache_name: 缓存名称
        :return: 缓存的数据
        """
        cache_file = self.get_cache_path(cache_name)
        print(f"📦 使用缓存数据: {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def write_cache(self, cache_name: str, data: Any, hours: int = 168) -> None:
        """
        写入缓存数据和元数据
        :param cache_name: 缓存名称
        :param data: 要缓存的数据
        :param hours: 缓存有效期（小时）
        """
        cache_file = self.get_cache_path(cache_name)
        meta_file = self.get_meta_path(cache_name)
        
        # 写入数据文件（纯数据，不包含元信息）
        print(f"💾 保存缓存到: {cache_file}")
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # 写入元数据文件
        now = datetime.now()
        expires_at = now + timedelta(hours=hours)
        meta_data = {
            'cache_name': cache_name,
            'created_at': now.isoformat(),
            'expires_at': expires_at.isoformat(),
            'ttl_hours': hours,
            'version': '1.0'
        }
        
        print(f"📝 保存元数据到: {meta_file}")
        with open(meta_file, 'w', encoding='utf-8') as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
    
    def clear_cache(self, cache_name: str) -> bool:
        """
        清除指定缓存（包括数据文件和元数据文件）
        :param cache_name: 缓存名称
        :return: 是否成功
        """
        cache_file = self.get_cache_path(cache_name)
        meta_file = self.get_meta_path(cache_name)
        
        success = False
        if cache_file.exists():
            cache_file.unlink()
            print(f"🗑️  已清除缓存: {cache_file}")
            success = True
        
        if meta_file.exists():
            meta_file.unlink()
            print(f"🗑️  已清除元数据: {meta_file}")
            success = True
            
        return success
    
    def clear_all_caches(self) -> int:
        """
        清除所有缓存（包括数据文件和元数据文件）
        :return: 清除的文件数量
        """
        count = 0
        # 清除所有 .json 数据文件
        for cache_file in self.cache_dir.glob("*.json"):
            # 跳过元数据文件
            if not cache_file.name.endswith('.meta.json'):
                cache_file.unlink()
                count += 1
        
        # 清除所有 .meta.json 元数据文件
        for meta_file in self.cache_dir.glob("*.meta.json"):
            meta_file.unlink()
            count += 1
        
        print(f"🗑️  已清除 {count} 个缓存文件")
        return count


def file_cached(cache_name: str, hours: int = 168, cache_dir: Optional[str] = None):
    """
    文件缓存装饰器
    将函数返回值缓存到文件中，支持自定义过期时间
    
    使用示例：
    @file_cached('stock_list', hours=168)
    def get_stock_list():
        # 耗时的数据获取操作
        return data
    
    :param cache_name: 缓存名称
    :param hours: 缓存有效期（小时），默认168小时（7天）
    :param cache_dir: 缓存目录，默认为 instock/cache/
    """
    def decorator(func: Callable) -> Callable:
        cache_manager = FileCache(cache_dir)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 检查缓存是否有效
            if cache_manager.is_cache_valid(cache_name, hours):
                return cache_manager.read_cache(cache_name)
            
            # 缓存不存在或已过期，执行函数
            print(f"🔄 缓存不存在或已过期，正在获取数据...")
            result = func(*args, **kwargs)
            
            # 保存到缓存（传入 hours 参数）
            cache_manager.write_cache(cache_name, result, hours)
            
            return result
        
        # 添加缓存管理方法
        wrapper.clear_cache = lambda: cache_manager.clear_cache(cache_name)
        wrapper.cache_info = lambda: {
            'cache_name': cache_name,
            'cache_file': str(cache_manager.get_cache_path(cache_name)),
            'is_valid': cache_manager.is_cache_valid(cache_name, hours),
            'hours': hours
        }
        
        return wrapper
    
    return decorator


# 创建全局缓存管理器实例
cache_manager = FileCache()
