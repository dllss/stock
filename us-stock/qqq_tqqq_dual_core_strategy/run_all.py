#!/usr/bin/env python3
"""统一入口：一条命令同时跑两条线。

线1 回测（python main.py）：后复权历史回测，验证策略长期有效性。
线2 实盘跟踪（python live_track.py）：跟踪真实账户收益 + 次日操作指令。

运行: python run_all.py [--end YYYY-MM-DD]
  --end  回测终点日期（默认今天），会同时透传给两条线。
"""

import argparse
import sys

import main


def run(end=None):
    print("\n" + "=" * 70)
    print("【线1+线2】回测 + 实盘跟踪（统一运行）")
    print("=" * 70)
    # main.main() 内部已调用 live_track.run(logger) 复用回测 logger，
    # 实盘历史表嵌入 HTML 报告，实盘日志合并进 backtest_full_{ts}.log。
    html_path = main.main(end=end)

    # 回测 HTML 报告路径醒目提示
    print("\n" + "#" * 70)
    if html_path:
        print("# 回测 HTML 报告已生成（含实盘跟踪章节）:")
        print(f"#   {html_path}")
    else:
        print("# 回测 HTML 报告本次未生成（见上方 warning）。")
    print("#" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QQQ & TQQQ 双核策略：回测 + 实盘跟踪 统一入口")
    parser.add_argument("--end", default=None, help="回测终点日期 (默认今天)，同时透传给两条线")
    args = parser.parse_args()
    run(end=args.end)
