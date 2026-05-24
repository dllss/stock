#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
除权股票前复权 K 线修复任务。

执行目的：
1. 在技术指标、K 线形态、策略选股计算前，修复发生除权除息股票的历史 K 线价格。
2. 使用东方财富前复权数据（qfq）覆盖 cn_stock_spot 中的 K 线字段。
3. 只更新价格/成交量/涨跌幅等 K 线字段，保留行业、市值、估值、财务等快照字段。

为什么不能整行替换 cn_stock_spot：
cn_stock_spot 同时保存行情快照、估值、市值、财务、行业等字段；
历史 K 线接口只返回 OHLCV 和涨跌幅字段，整行替换会导致非 K 线字段丢失。
"""

# 标准库：日期窗口计算、日志、路径、命令行路径注入、类型标注。
import datetime
import logging
import os.path
import sys
from typing import List, Tuple

# 第三方库：用于接收、清洗和批量处理 K 线数据。
import pandas as pd

# 兼容直接运行脚本：
# 当前文件位于 instock/job/，需要把项目根目录加入 sys.path，
# 否则在 Windows/命令行直接执行时可能找不到 instock 包。
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

# 表结构定义：提供 cn_stock_bonus、cn_stock_spot 的真实表名。
import instock.core.tablestructure as tbs
# 请求延迟配置：每处理一只股票后按项目统一节流规则 sleep，避免触发接口限流。
from instock.config.delay_manager import sleep_with_delay
# 东方财富 K 线抓取模块：这里只复用其中的 fetcher，请求底层 K 线接口。
from instock.core.crawling import stock_hist_em
from instock.core.crawling.kline_utils import KLINE_COLUMNS as HIST_COLUMNS, apply_kline_columns
from instock.core.crawling.market_utils import get_market_id
# 数据库封装：检查表、查询除权股票、更新 cn_stock_spot。
import instock.lib.database as mdb
# 任务运行模板：统一处理命令行日期参数、交易日判断、区间执行。
import instock.lib.run_template as runt
# 交易日工具：复用 get_trade_hist_interval 获取指标计算用的历史窗口。
import instock.lib.trade_time as trd

# cn_stock_spot 中允许被本任务更新的字段。
# 这些字段全部来自东方财富历史 K 线接口。
# 注意：不要把 industry、total_market_cap、pe、pb 等快照/财务字段放进来。
KLINE_UPDATE_COLUMNS = (
    "open_price",
    "new_price",
    "high_price",
    "low_price",
    "volume",
    "deal_amount",
    "amplitude",
    "change_rate",
    "ups_downs",
    "turnoverrate",
)

def parse_ex_dividend_lookback_days(default_days: int = 30) -> int:
    """
    解析除权候选扫描天数配置。

    环境变量配置错误时不要让模块 import 失败；
    回退到默认值，保证 execute_daily_job 还能继续运行。
    """
    raw_value = os.environ.get("INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS", str(default_days))
    try:
        lookback_days = int(raw_value)
    except (TypeError, ValueError):
        logging.warning(
            f"Invalid INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS={raw_value!r}, fallback to {default_days}"
        )
        return default_days

    if lookback_days < 0:
        logging.warning(
            f"INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS must be non-negative: {lookback_days}, "
            f"fallback to {default_days}"
        )
        return default_days

    return lookback_days


# 单独执行本任务时，默认扫描运行日期往前 30 天内发生除权除息的股票。
# 命中股票后，实际修复的 K 线范围仍然是完整指标窗口。
# 可通过环境变量覆盖，例如：
# INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS=0 只扫描运行当天；
# INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS=90 扫描近 90 天。
EX_DIVIDEND_LOOKBACK_DAYS = parse_ex_dividend_lookback_days()

HIST_COLUMN_MAPPING = {
    "日期": "date",
    "开盘": "open_price",
    "收盘": "new_price",
    "最高": "high_price",
    "最低": "low_price",
    "成交量": "volume",
    "成交额": "deal_amount",
    "振幅": "amplitude",
    "涨跌幅": "change_rate",
    "涨跌额": "ups_downs",
    "换手率": "turnoverrate",
}


def _to_date(value) -> datetime.date:
    """
    将任务模板传入的日期统一转换为 datetime.date。

    支持类型：
    - datetime.datetime：取 date 部分。
    - datetime.date：直接返回。
    - str：按 YYYY-MM-DD 解析。

    不支持的类型直接抛错，避免静默生成错误修复窗口。
    """
    # datetime.datetime 也是 datetime.date 的子类，所以要先判断 datetime.datetime。
    if isinstance(value, datetime.datetime):
        return value.date()
    # 已经是 date 类型时无需转换。
    if isinstance(value, datetime.date):
        return value
    # 命令行日期通常会被 run_template 解析为 date；这里保留字符串兼容。
    if isinstance(value, str):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    # 传入未知类型说明调用方有问题，显式报错更安全。
    raise ValueError(f"Unsupported date value: {value!r}")


def get_repair_window(run_date) -> Tuple[datetime.date, datetime.date]:
    """
    获取需要修复 K 线数据的完整历史窗口。

    技术指标、K 线形态、策略选股读取 stock_hist_data。
    stock_hist_data 内部使用 trd.get_trade_hist_interval 计算历史窗口。
    本任务使用同一套窗口，确保修复范围和后续计算读取范围一致。

    返回：
    - start_date：指标计算历史窗口开始日期。
    - end_date：本次任务运行日期。
    """
    # 统一日期类型，避免字符串和 date 混用。
    end_date = _to_date(run_date)
    # 获取项目已有的历史 K 线窗口起点，格式为 YYYYMMDD。
    start_date_str, _ = trd.get_trade_hist_interval(end_date.strftime("%Y-%m-%d"))
    # 转成 date，便于后续窗口比较和接口参数格式化。
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d").date()
    return start_date, end_date


def get_ex_dividend_query_window(
    repair_start_date: datetime.date,
    end_date: datetime.date,
    lookback_days: int = EX_DIVIDEND_LOOKBACK_DAYS,
) -> Tuple[datetime.date, datetime.date]:
    """
    获取除权股票候选查询窗口。

    这里的窗口只用于判断“哪些股票最近发生过除权除息”。
    不等于实际更新 K 线数据的窗口。

    设计原因：
    - 每天只需要处理最近发生公司行为的股票，避免扫描多年除权记录导致任务过慢。
    - 一旦股票命中候选，再用 get_repair_window 的完整历史窗口修复 K 线。
    """
    # 候选起点 = 运行日期 - lookback_days。
    candidate_start_date = end_date - datetime.timedelta(days=lookback_days)
    # 候选起点不能早于实际修复窗口起点，避免查询范围超过后续可修复范围。
    if candidate_start_date < repair_start_date:
        candidate_start_date = repair_start_date
    return candidate_start_date, end_date


def get_ex_dividend_stocks(start_date: datetime.date, end_date: datetime.date) -> List[Tuple[str, str]]:
    """
    从 cn_stock_bonus 查询指定窗口内发生除权除息的股票。

    判断依据：
    - ex_dividend_date IS NOT NULL
    - ex_dividend_date 在 [start_date, end_date] 内

    返回：
    - [(code, name), ...]

    注意：
    这里判断的是“公司行为表记录了除权除息日”，不是“价格跳变检测命中”。
    """
    # 从表结构定义读取真实表名，避免硬编码表名到处散落。
    table_name = tbs.TABLE_CN_STOCK_BONUS["name"]
    # 分红配送任务还没建表时，直接跳过修复，避免中断完整 execute_daily_job。
    if not mdb.checkTableIsExist(table_name):
        logging.info("cn_stock_bonus does not exist, skip ex-dividend K-line repair")
        return []

    # 查询窗口内发生过除权除息的股票。
    # GROUP BY code：同一股票多条分红记录只修复一次。
    # COALESCE(MAX(name), '')：尽量保留名称用于日志。
    # 日期值使用参数化查询，避免拼接日期字符串。
    sql = f"""
        SELECT
            `code`,
            COALESCE(MAX(`name`), '') AS `name`
        FROM `{table_name}`
        WHERE `ex_dividend_date` IS NOT NULL
          AND `ex_dividend_date` >= %s
          AND `ex_dividend_date` <= %s
        GROUP BY `code`
        ORDER BY MIN(`ex_dividend_date`) ASC
    """
    # 传入 YYYY-MM-DD 字符串，MySQL DATE 字段可直接比较。
    rows = mdb.executeSqlFetch(sql, (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
    # 没有候选股票时返回空列表，让上层正常结束。
    if not rows:
        return []
    # 统一转成字符串，避免数据库驱动返回 bytes/int 等类型影响后续拼 secid。
    return [(str(code), str(name or "")) for code, name in rows]


def fetch_qfq_hist_data(code: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    """
    直接调用东方财富历史 K 线接口获取单只股票前复权日线。

    这里不复用 stock_hist_em.stock_zh_a_hist()，因为它先加载全市场代码映射；
    映射缓存异常时会让所有修复任务失败。当前函数只需要单只股票代码，
    可以直接构造 secid 请求接口。

    参数：
    - code：股票代码。
    - start_date：K 线开始日期。
    - end_date：K 线结束日期。

    返回：
    - 东方财富原始列名 DataFrame；空数据返回空 DataFrame。
    """
    # 东方财富日 K 接口地址，与 stock_hist_em.stock_zh_a_hist 使用同一个接口。
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    # fqt=1 表示前复权；klt=101 表示日线。
    # f51-f61 分别对应日期、开高低收、成交量、成交额、振幅、涨跌幅、涨跌额、换手率；
    # f116 是历史代码保留字段，当前接口通常不返回，解析层会兼容 11/12 列。
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": "1",
        "secid": f"{get_market_id(code)}.{code}",
        "beg": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
        "_": "1623766962675",
    }
    # 复用项目统一 fetcher，保留已有代理、重试、日志、限速等请求能力。
    response = stock_hist_em.fetcher.make_request(url, params=params)
    # 接口返回 JSON；没有 data/klines 时认为无有效 K 线。
    data_json = response.json()
    if not (data_json.get("data") and data_json["data"].get("klines")):
        return pd.DataFrame()

    # klines 是逗号拼接字符串列表，需要拆成二维表。
    data = pd.DataFrame([item.split(",") for item in data_json["data"]["klines"]])
    # 设置为东方财富原始中文列名，后续统一用 HIST_COLUMN_MAPPING 转换。
    return apply_kline_columns(data)


def normalize_qfq_hist_data(hist_data: pd.DataFrame, code: str) -> pd.DataFrame:
    """
    将东方财富前复权 K 线数据转换为 cn_stock_spot 可更新字段。

    处理内容：
    1. 中文列名转数据库字段名。
    2. 添加 code 字段。
    3. 只保留 KLINE_UPDATE_COLUMNS 中声明的 K 线字段。
    4. 日期标准化为 YYYY-MM-DD。
    5. 数值字段转 numeric，非法值转 NaN。

    返回：
    - 包含 date、code、KLINE_UPDATE_COLUMNS 的 DataFrame。
    """
    # 统一输出列顺序，便于生成 UPDATE 参数。
    columns = ["date", "code", *KLINE_UPDATE_COLUMNS]
    # 空输入直接返回空结构，避免上层判断字段时报错。
    if hist_data is None or hist_data.empty:
        return pd.DataFrame(columns=columns)

    # 按映射表把中文字段名改成 cn_stock_spot 字段名。
    data = hist_data.rename(columns=HIST_COLUMN_MAPPING).copy()
    # 接口返回不包含股票代码，手动补充。
    data["code"] = code

    # 只选择需要更新的字段，避免把未知字段写入数据库。
    available_columns = [column for column in columns if column in data.columns]
    data = data[available_columns]

    # 如果接口缺少某些字段，用 None 补齐，保持 SQL 参数数量稳定。
    missing_columns = [column for column in columns if column not in data.columns]
    for column in missing_columns:
        data[column] = None

    # 固定列顺序：date、code、K线字段。
    data = data[columns]
    # 将日期统一为字符串，匹配 cn_stock_spot 的 date 查询条件。
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 将所有 K 线字段转为数值；无法解析的值转为 NaN，写库时会转 None。
    for column in KLINE_UPDATE_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    # date/code 缺失的行无法定位数据库记录，必须丢弃。
    return data.dropna(subset=["date", "code"])


def build_update_sql(table_name: str = tbs.TABLE_CN_STOCK_SPOT["name"]) -> str:
    """
    构造只更新 K 线字段的 SQL。

    WHERE 条件使用 code + date，对应 cn_stock_spot 主键。
    不包含非 K 线字段，避免覆盖行情快照中的行业、市值、估值等信息。
    """
    # 生成形如 `open_price` = %s, `new_price` = %s 的 SET 子句。
    set_clause = ", ".join([f"`{column}` = %s" for column in KLINE_UPDATE_COLUMNS])
    # 参数顺序必须和 build_update_params 保持一致。
    return f"UPDATE `{table_name}` SET {set_clause} WHERE `code` = %s AND `date` = %s"


def _db_value(value):
    """
    将 pandas/numpy 值转换为数据库驱动可接受的 Python 值。

    - NaN/NaT 转 None，写入 SQL NULL。
    - numpy scalar 使用 item() 转成 Python scalar。
    """
    # pandas 用 NaN 表示缺失；PyMySQL 需要 None 才能写 NULL。
    if pd.isna(value):
        return None
    # numpy 标量转 Python 标量，避免数据库驱动类型兼容问题。
    return value.item() if hasattr(value, "item") else value


def build_update_params(row: pd.Series) -> Tuple:
    """
    按 build_update_sql 的占位符顺序构造一行 UPDATE 参数。

    参数顺序：
    1. KLINE_UPDATE_COLUMNS 对应的值。
    2. code。
    3. date。
    """
    # 按固定字段顺序提取 K 线值，确保和 SQL SET 子句一一对应。
    values = tuple(_db_value(row[column]) for column in KLINE_UPDATE_COLUMNS)
    # WHERE 条件放在最后：code + date。
    return (*values, str(row["code"]), str(row["date"]))


def update_cn_stock_spot_kline_data(data: pd.DataFrame) -> int:
    """
    批量更新 cn_stock_spot 的 K 线字段。

    返回：
    - 数据库 cursor.rowcount，表示本次 executemany 影响行数。
    """
    # 没有数据时直接返回 0，调用方可继续处理下一只股票。
    if data is None or data.empty:
        return 0

    # 从表结构定义读取表名，避免硬编码。
    table_name = tbs.TABLE_CN_STOCK_SPOT["name"]
    # cn_stock_spot 不存在说明基础数据还没初始化，不能修复。
    if not mdb.checkTableIsExist(table_name):
        logging.warning("cn_stock_spot does not exist, skip ex-dividend K-line update")
        return 0

    # 构造一次 SQL，多行参数使用 executemany 批量更新。
    sql = build_update_sql(table_name)
    params = [build_update_params(row) for _, row in data.iterrows()]

    # 使用原生连接执行批量 UPDATE；数据库配置 autocommit=True。
    with mdb.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, params)
            # rowcount 用于日志统计，便于确认修复是否实际命中历史记录。
            return cursor.rowcount


def repair_one_stock(code: str, name: str, start_date: datetime.date, end_date: datetime.date) -> int:
    """
    修复单只股票在指定历史窗口内的前复权 K 线。

    步骤：
    1. 拉取前复权历史 K 线。
    2. 标准化为 cn_stock_spot K 线字段。
    3. 批量 UPDATE cn_stock_spot。
    """
    # 记录当前股票和修复窗口，方便定位接口或数据库失败。
    logging.info(f"Repair qfq K-line data: {code} {name} ({start_date} to {end_date})")
    # 拉取东方财富前复权日线。
    hist_data = fetch_qfq_hist_data(code, start_date, end_date)
    # 转换为 cn_stock_spot 可更新字段。
    normalized_data = normalize_qfq_hist_data(hist_data, code)
    # 停牌、退市、接口无数据等情况可能返回空表；跳过即可。
    if normalized_data.empty:
        logging.warning(f"No qfq K-line data fetched for {code} {name}")
        return 0
    # 批量更新数据库，并返回影响行数。
    return update_cn_stock_spot_kline_data(normalized_data)


def repair_ex_dividend_kline_data(run_date, ex_dividend_start_date=None, ex_dividend_end_date=None):
    """
    修复指定运行日期关联的除权股票 K 线。

    run_template 会根据命令行参数多次调用本函数：
    - 无参数：最新交易日。
    - 单日：指定日期。
    - 多日/区间：逐个交易日执行。
    """
    # 完整修复窗口：和后续指标计算读取历史 K 线的窗口一致。
    start_date, end_date = get_repair_window(run_date)
    # 候选窗口：只扫描近期发生除权的股票，减少接口请求数量。
    if ex_dividend_start_date is None and ex_dividend_end_date is None:
        ex_dividend_start_date, ex_dividend_end_date = get_ex_dividend_query_window(start_date, end_date)
    else:
        if ex_dividend_start_date is None or ex_dividend_end_date is None:
            raise ValueError("ex_dividend_start_date and ex_dividend_end_date must be provided together")
        ex_dividend_start_date = _to_date(ex_dividend_start_date)
        ex_dividend_end_date = _to_date(ex_dividend_end_date)
        if ex_dividend_start_date < start_date:
            ex_dividend_start_date = start_date
    # 从 cn_stock_bonus 查询候选股票。
    stocks = get_ex_dividend_stocks(ex_dividend_start_date, ex_dividend_end_date)
    # 没有候选股票时，本任务正常结束。
    if not stocks:
        logging.info(f"No ex-dividend stocks found from {ex_dividend_start_date} to {ex_dividend_end_date}")
        return

    # 日志里同时打印候选窗口和实际 K 线修复窗口，避免误解。
    logging.info(
        f"Found {len(stocks)} ex-dividend stocks from {ex_dividend_start_date} to {ex_dividend_end_date}, "
        f"repair qfq K-line window {start_date} to {end_date}"
    )
    # success_count 表示接口和数据库流程没有抛异常的股票数。
    success_count = 0
    # updated_rows 表示实际更新到 cn_stock_spot 的行数。
    updated_rows = 0

    # 串行处理，配合 sleep_with_delay 降低接口限流风险。
    for index, (code, name) in enumerate(stocks, 1):
        try:
            # 修复单只股票，并累计实际更新行数。
            row_count = repair_one_stock(code, name, start_date, end_date)
            updated_rows += row_count
            success_count += 1
            logging.info(f"[{index}/{len(stocks)}] repaired {code} {name}, updated rows: {row_count}")
        except Exception as e:
            # 单只股票失败不阻断后续股票，完整任务最后给出汇总。
            logging.error(f"[{index}/{len(stocks)}] repair failed: {code} {name}: {e}", exc_info=True)
        finally:
            # 最后一只股票后无需等待。
            if index < len(stocks):
                sleep_with_delay("normal")

    # 输出任务汇总，方便从日志判断实际修复效果。
    logging.info(
        f"Ex-dividend K-line repair finished: stocks={len(stocks)}, "
        f"success={success_count}, updated_rows={updated_rows}"
    )


def main(ex_dividend_start_date=None, ex_dividend_end_date=None):
    """
    脚本入口。

    使用 run_template 统一处理命令行日期参数：
    python adjustment_data_daily_job.py
    python adjustment_data_daily_job.py 2026-05-20
    python adjustment_data_daily_job.py 2026-05-01 2026-05-20
    """
    # 延迟导入任务日志工具，避免模块导入时产生任务日志。
    from instock.job.task_utils import log_task_start

    # 写入统一格式的任务开始日志。
    log_task_start("adjustment_kline_repair", "修复除权股票前复权K线数据")
    # 将具体日期解析和交易日过滤交给项目已有运行模板。
    # execute_daily_job 可传入明确的除权查询窗口；单独执行时不传，默认查近 30 天。
    runt.run_with_args(repair_ex_dividend_kline_data, ex_dividend_start_date, ex_dividend_end_date)


# 仅直接运行本文件时执行；被 execute_daily_job import 时不会自动执行。
if __name__ == "__main__":
    # 直接运行时初始化日志输出到控制台和 instock/log/stock_execute_job.log。
    from instock.lib.logger_config import setup_job_logging

    setup_job_logging()
    # 启动任务。
    main()
