#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
清理 stock_execute_job.log 中指定日期之前的日志。

默认语义：
- 删除 cutoff_date 之前的日志。
- 保留 cutoff_date 当天及之后的日志。
- 按日志块清理：异常堆栈等没有时间戳的续行会跟随上一条有时间戳的日志一起保留或删除。
- 默认先生成 stock_execute_job.log.bak，再覆盖原日志。
"""

import argparse
import datetime
import logging
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from typing import List, Optional, Tuple


# 兼容直接运行脚本：
# python instock/job/cleanup_stock_execute_log.py 2026-05-20
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)


# logger_config.py 使用的日志格式：
# 2026-05-24 19:54:29,463 [database.py:582 - checkTableIsExist()] ...
LOG_TIMESTAMP_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2},\d{3} ")


@dataclass
class CleanupResult:
    """日志清理结果，用于命令行输出和单元测试断言。"""

    log_path: str
    cutoff_date: datetime.date
    kept_lines: int
    removed_lines: int
    backup_path: Optional[str]
    dry_run: bool


def default_log_path() -> str:
    """返回项目默认的 stock_execute_job.log 路径。"""
    instock_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(instock_dir, "log", "stock_execute_job.log")


def parse_cutoff_date(value: str) -> datetime.date:
    """解析命令行传入的 YYYY-MM-DD 日期。"""
    try:
        return datetime.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"日期格式错误，应为 YYYY-MM-DD: {value}") from exc


def split_lines_by_cutoff(lines: List[str], cutoff_date: datetime.date) -> Tuple[List[str], int]:
    """
    按 cutoff_date 拆分日志行。

    有时间戳的行决定当前日志块是否保留；
    没有时间戳的续行沿用上一条日志块的保留/删除状态。
    """
    kept_lines = []
    removed_lines = 0
    keep_current_block = True

    for line in lines:
        timestamp_match = LOG_TIMESTAMP_PATTERN.match(line)
        if timestamp_match:
            try:
                line_date = datetime.date.fromisoformat(timestamp_match.group(1))
                keep_current_block = line_date >= cutoff_date
            except ValueError:
                logging.debug(f"Ignore malformed log timestamp: {timestamp_match.group(1)}")

        if keep_current_block:
            kept_lines.append(line)
        else:
            removed_lines += 1

    return kept_lines, removed_lines


def write_lines_atomically(log_path: str, lines: List[str]) -> None:
    """先写临时文件，再原子替换原日志，避免中途失败造成日志损坏。"""
    log_dir = os.path.dirname(os.path.abspath(log_path)) or "."
    log_name = os.path.basename(log_path)
    temp_path = None

    try:
        fd, temp_path = tempfile.mkstemp(prefix=f"{log_name}.", suffix=".tmp", dir=log_dir)
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as temp_file:
            temp_file.writelines(lines)
        os.replace(temp_path, log_path)
        temp_path = None
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


def cleanup_log_before_date(
    log_path: str,
    cutoff_date: datetime.date,
    backup_path: Optional[str] = None,
    dry_run: bool = False,
    create_backup: bool = True,
) -> CleanupResult:
    """
    删除日志文件中 cutoff_date 之前的日志。

    参数：
    - log_path：待清理日志文件。
    - cutoff_date：保留此日期当天及之后的日志。
    - backup_path：备份路径；不传时默认使用 <log_path>.bak。
    - dry_run：只统计不写文件。
    - create_backup：是否在写入前创建备份。
    """
    if not os.path.exists(log_path):
        raise FileNotFoundError(f"日志文件不存在: {log_path}")

    with open(log_path, "r", encoding="utf-8") as log_file:
        original_lines = log_file.readlines()

    kept_lines, removed_lines = split_lines_by_cutoff(original_lines, cutoff_date)
    resolved_backup_path = backup_path or f"{log_path}.bak"

    if not dry_run and removed_lines > 0:
        if create_backup:
            shutil.copy2(log_path, resolved_backup_path)
        else:
            resolved_backup_path = None
        write_lines_atomically(log_path, kept_lines)
    elif dry_run or not create_backup or removed_lines == 0:
        resolved_backup_path = None

    return CleanupResult(
        log_path=log_path,
        cutoff_date=cutoff_date,
        kept_lines=len(kept_lines),
        removed_lines=removed_lines,
        backup_path=resolved_backup_path,
        dry_run=dry_run,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """构造命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        description="清理 stock_execute_job.log 中指定日期之前的日志，保留该日期当天及之后的日志。"
    )
    parser.add_argument("cutoff_date", type=parse_cutoff_date, help="截止日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--log-file",
        default=default_log_path(),
        help="日志文件路径，默认 instock/log/stock_execute_job.log",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印清理统计，不修改日志文件")
    parser.add_argument("--no-backup", action="store_true", help="清理前不生成 .bak 备份")
    return parser


def main(argv=None) -> int:
    """命令行入口。"""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    result = cleanup_log_before_date(
        log_path=os.path.abspath(args.log_file),
        cutoff_date=args.cutoff_date,
        dry_run=args.dry_run,
        create_backup=not args.no_backup,
    )

    print(f"日志文件: {result.log_path}")
    print(f"截止日期: {result.cutoff_date}（删除此日期之前，保留当天及之后）")
    print(f"删除行数: {result.removed_lines}")
    print(f"保留行数: {result.kept_lines}")
    if result.dry_run:
        print("模式: dry-run，未修改日志文件")
    elif result.backup_path:
        print(f"备份文件: {result.backup_path}")
    else:
        print("备份文件: 未生成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
