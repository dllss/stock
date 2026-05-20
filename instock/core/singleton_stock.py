#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据缓存模块（单例模式）
===========================
这个模块提供股票数据的单例缓存，避免重复获取相同的数据。

单例模式的好处：
1. 内存优化：数据只加载一次，所有地方共享使用
2. 性能提升：避免重复的网络请求和数据处理
3. 数据一致性：确保整个系统使用的是同一份数据

模块包含两个单例类：
1. stock_data：当天的股票数据（快照数据）
2. stock_hist_data：股票的历史K线数据（用于技术分析）

数据流程：
网络请求 → 数据抓取 → 单例缓存 → 多个模块共享使用
"""

# ==================== 导入必需的库 ====================
import logging  # 日志记录
import time  # 时间处理（用于性能统计）
import pandas as pd  # 数据处理
import instock.core.stockfetch as stf  # 股票数据抓取模块
import instock.core.tablestructure as tbs  # 数据表结构定义
import instock.lib.trade_time as trd  # 交易时间处理模块
import instock.lib.database as mdb  # 数据库操作模块
from instock.lib.singleton_type import singleton_type  # 单例模式元类

__author__ = 'myh '
__date__ = '2023/3/10 '


# ==================== 当天股票数据单例类 ====================
class stock_data(metaclass=singleton_type):
    """
    当天股票数据的单例类
    
    功能说明：
        - 获取指定日期的所有股票的快照数据
        - 包括：代码、名称、价格、涨跌幅、成交量、市值等
        - 使用单例模式，同一个日期的数据只加载一次
        - 优先从数据库读取，数据库无数据时才从网络抓取
        
    数据获取策略（优化后）：
        1. 首先尝试从数据库 cn_stock_spot 表查询
        2. 如果数据库有数据，直接返回（快速）
        3. 如果数据库无数据，从网络API抓取
        4. 抓取成功后可选保存到数据库
        
    使用示例：
        from datetime import date
        
        # 第一次调用：优先查数据库，无数据则从网络抓取
        stock_obj = stock_data(date(2024, 1, 1))
        df = stock_obj.get_data()
        
        # 第二次调用：直接返回缓存的数据（不会重新查询）
        stock_obj2 = stock_data(date(2024, 1, 1))
        df2 = stock_obj2.get_data()  # df2和df是同一个对象
        
    单例模式实现：
        通过metaclass=singleton_type实现
        相同参数的多次实例化会返回同一个对象
    """
    
    def __init__(self, date):
        """
        初始化股票数据单例（优化版：先查数据库）
        
        参数说明：
            date (datetime.date): 要获取数据的日期
            
        执行流程（优化后）：
            1. 尝试从数据库 cn_stock_spot 表查询该日期数据
            2. 如果数据库有数据，直接使用（快速）
            3. 如果数据库无数据，调用网络API抓取
            4. 存储到self.data中
            
        异常处理：
            如果查询和抓取都失败，记录错误日志
        """
        self.data = None
        
        try:
            # 步骤1: 格式化日期
            if isinstance(date, str):
                date_str = date
                date_obj = pd.to_datetime(date).date()
            else:
                date_str = date.strftime('%Y-%m-%d')
                date_obj = date
            
            # 步骤2: 尝试从数据库读取
            table_name = tbs.TABLE_CN_STOCK_SPOT['name']  # 'cn_stock_spot'
            
            if mdb.checkTableIsExist(table_name):
                logging.info(f"🔍 尝试从数据库读取 {date_str} 的股票数据...")
                
                # 构建SQL查询
                sql = f"SELECT * FROM `{table_name}` WHERE `date` = '{date_str}'"
                
                try:
                    # 执行查询
                    data_from_db = pd.read_sql(sql=sql, con=mdb.engine())
                    
                    if data_from_db is not None and len(data_from_db) > 0:
                        logging.info(f"✅ 从数据库成功读取 {len(data_from_db)} 条股票数据: {date_str}")
                        
                        # 确保 date 列是字符串格式（与网络抓取的数据保持一致）
                        if 'date' in data_from_db.columns:
                            # 如果 date 列是 datetime 类型，转换为字符串
                            if hasattr(data_from_db['date'].iloc[0], 'strftime'):
                                data_from_db['date'] = data_from_db['date'].apply(
                                    lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else str(x)
                                )
                        
                        self.data = data_from_db
                        return  # 数据库有数据，直接返回
                    
                    else:
                        logging.info(f"⚠️  数据库中无 {date_str} 的数据")
                        
                except Exception as db_error:
                    logging.warning(f"⚠️  数据库查询失败: {db_error}")
            
            # 步骤3: 数据库无数据或查询失败，从网络API抓取实时数据
            logging.info(f"🌐 数据库中无 {date_str} 的数据，开始从网络API抓取...")
            
            try:
                # 调用stockfetch模块获取实时股票数据
                data_from_api = stf.fetch_stocks(date_obj)
                
                if data_from_api is not None and len(data_from_api) > 0:
                    logging.info(f"✅ 从网络API成功抓取 {len(data_from_api)} 条股票数据: {date_str}")
                    self.data = data_from_api
                    return  # 网络抓取成功，直接返回
                else:
                    logging.error(f"❌ 从网络API抓取失败，返回数据为空")
                    raise RuntimeError(f"无法获取 {date_str} 的股票数据（网络API返回空）")
                    
            except Exception as api_error:
                logging.error(f"❌ 网络API抓取失败: {api_error}")
                raise RuntimeError(f"无法获取 {date_str} 的股票数据：{api_error}")
                
        except RuntimeError:
            # RuntimeError 是我们主动抛出的，需要向上传播
            raise
        except Exception as e:
            # 记录错误日志
            logging.error(f"singleton.stock_data处理异常：{e}", exc_info=True)

    def get_data(self):
        """
        获取股票数据
        
        返回值：
            pandas.DataFrame: 股票数据表
                - 行：每只股票
                - 列：股票的各项指标（代码、名称、价格等）
                
        DataFrame结构示例：
            code    name    new_price  change_rate  volume  ...
            600000  浦发银行   10.5      1.5          1000000  ...
            600001  邯郸钢铁   5.2       -0.8         500000   ...
            ...
        """
        return self.data


# ==================== 股票历史数据单例类 ====================
class stock_hist_data(metaclass=singleton_type):
    """
    股票历史K线数据的单例类
    
    功能说明：
        - 获取多只股票的历史K线数据
        - 使用串行方式获取，避免被反爬识别
        - 数据用于计算技术指标、K线形态识别、策略选股等
        
    K线数据包含：
        - 日期
        - 开盘价、最高价、最低价、收盘价（OHLC）
        - 成交量、成交额
        - 涨跌幅等
        
    串行执行说明：
        假设要获取3000只股票的历史数据
        - 每次请求前延迟9-15秒，避免触发反爬机制
        - 虽然速度较慢，但稳定可靠，不会被封禁
        - 首次运行后数据会缓存，后续运行速度大幅提升
        
    使用示例：
        # 获取所有股票的历史数据
        hist_obj = stock_hist_data(date='2024-01-01')
        hist_dict = hist_obj.get_data()
        
        # hist_dict是字典，键是(日期,代码)，值是DataFrame
        for (date, code), df in hist_dict.items():
            print(f"{code}的历史数据：")
            print(df.head())  # 显示前5行
    """
    
    def __init__(self, date=None, stocks=None):
        """
        初始化股票历史数据单例
        
        参数说明：
            date (str, 可选): 结束日期，格式"YYYY-MM-DD"
                              历史数据会获取[结束日期-3年, 结束日期]的数据
            stocks (list, 可选): 股票列表，格式[(date, code, name), ...]
                                如果为None，则获取当天所有股票的历史数据
                          
        执行流程：
            1. 如果没有指定stocks，从当天数据中获取所有股票列表
            2. 计算历史数据的时间范围（向前推3年）
            3. 串行获取每只股票的历史数据（避免反爬）
            4. 将结果存储到字典中
        """
        # 步骤1: 如果没有指定股票列表，获取当天所有股票
        if stocks is None:
            logging.info(f"🔍 开始获取 {date} 的股票列表...")
            # 从当天股票数据中提取需要的列（日期、代码、名称）
            _subset = stock_data(date).get_data()[list(tbs.TABLE_CN_STOCK_FOREIGN_KEY['columns'])]
            logging.info(f"✅ 成功获取股票列表，共 {len(_subset)} 只股票")
            # 将DataFrame转换为元组列表：[(date, code, name), ...]
            # values：获取DataFrame的值（numpy数组）
            # [tuple(x) for x in ...]：将每一行转换为元组
            stocks = [tuple(x) for x in _subset.values]
            logging.info(f"✅ 股票列表转换完成")
        
        # 检查股票列表是否为空
        if stocks is None:
            self.data = None
            return
        
        # 步骤2: 计算历史数据的时间范围
        # 只计算一次，提高效率（所有股票使用相同的时间范围）
        # stocks[0][0]：第一只股票的日期
        logging.info(f"🔍 开始计算历史数据时间范围...")
        date_start, is_cache = trd.get_trade_hist_interval(stocks[0][0])
        logging.info(f"✅ 历史数据时间范围: {date_start} 到 {stocks[0][0]}")
        
        # 步骤3: 准备存储结果的字典
        _data = {}  # 键：(date, code)，值：历史数据DataFrame
        
        try:
            # 【性能优化】分批从数据库获取所有股票的历史数据
            total_stocks = len(stocks)
            logging.info(f"🔍 开始批量加载 {total_stocks} 只股票的历史数据...")
            
            # 计算结束日期和起始日期
            end_date = stocks[0][0]  # 使用第一个股票的日期作为结束日期
            date_start_result = trd.get_trade_hist_interval(stocks[0][0])  # 获取历史数据的起始日期
            # get_trade_hist_interval 返回的是 (date_str, bool) 元组，需要提取日期字符串
            if isinstance(date_start_result, tuple):
                date_start = date_start_result[0]
            else:
                date_start = date_start_result
            
            # 【关键优化】分批查询，每批500只股票，避免内存溢出和查询超时
            import instock.lib.database as mdb
            import pandas as pd
            
            batch_size = 500  # 每批处理的股票数量
            all_data_list = []  # 存储所有批次的数据
            
            for batch_idx in range(0, total_stocks, batch_size):
                batch_stocks = stocks[batch_idx:batch_idx + batch_size]
                stock_codes = [stock[1] for stock in batch_stocks]
                codes_str = ','.join([f"'{code}'" for code in stock_codes])
                
                sql = f"""
                    SELECT 
                        `code`,
                        `date`,
                        `open_price` as `open`,
                        `new_price` as `close`,
                        `high_price` as `high`,
                        `low_price` as `low`,
                        `volume` as `volume`,
                        `deal_amount` as `amount`,
                        `amplitude` as `振幅`,
                        `change_rate` as `p_change`,
                        `ups_downs` as `涨跌额`,
                        `turnoverrate` as `换手率`
                    FROM cn_stock_spot
                    WHERE `code` IN ({codes_str})
                      AND `date` >= '{date_start}'
                      AND `date` <= '{end_date}'
                    ORDER BY `code` ASC, `date` ASC
                """
                
                batch_num = batch_idx // batch_size + 1
                total_batches = (total_stocks + batch_size - 1) // batch_size
                logging.info(f"📊 正在执行第 {batch_num}/{total_batches} 批查询（{len(batch_stocks)} 只股票）...")
                
                query_start = time.time()
                batch_data = pd.read_sql(sql, con=mdb.engine())
                query_end = time.time()
                
                if batch_data is not None and len(batch_data) > 0:
                    all_data_list.append(batch_data)
                    logging.info(f"✅ 第 {batch_num} 批查询完成，耗时: {query_end - query_start:.2f}秒，获取 {len(batch_data)} 条记录")
                else:
                    logging.warning(f"⚠️ 第 {batch_num} 批查询未返回数据")
            
            # 合并所有批次的数据
            if not all_data_list:
                logging.warning("⚠️ 未查询到任何历史数据")
                self.data = None
                return
            
            logging.info(f"📊 正在合并所有批次的数据...")
            all_data = pd.concat(all_data_list, ignore_index=True)
            logging.info(f"✅ 数据合并完成，总共 {len(all_data)} 条记录")
            
            if all_data is None or len(all_data) == 0:
                logging.warning("⚠️ 未查询到任何历史数据")
                self.data = None
                return
            
            # 【关键】按股票代码分组，构建与原逻辑相同的数据结构
            logging.info(f"📊 正在按股票分组处理数据...")
            grouped = all_data.groupby('code')
            
            for idx, (code, group_df) in enumerate(grouped, 1):
                try:
                    # 复制数据避免只读问题
                    df_copy = group_df.copy(deep=True)
                    
                    # 转换日期格式为字符串（与原API返回格式一致）
                    df_copy['date'] = pd.to_datetime(df_copy['date']).dt.strftime('%Y-%m-%d')
                    
                    # 找到对应的股票元组 (date, code, name)
                    target_stock = None
                    for stock in stocks:
                        if stock[1] == code:
                            target_stock = stock
                            break
                    
                    if target_stock is not None:
                        _data[target_stock] = df_copy
                    
                    # 打印进度（每100只股票打印一次）
                    if idx % 100 == 0 or idx == len(grouped):
                        progress = idx * 100 // len(grouped)
                        logging.info(f"📊 数据处理进度: {idx}/{len(grouped)} ({progress}%)")
                        
                except Exception as e:
                    # 单只股票出错不影响其他股票
                    logging.error(f"处理股票 {code} 数据时出错: {e}")
            
            logging.info(f"✅ 所有股票历史数据加载完成")
                    
        except Exception as e:
            # 整体出错
            logging.error(f"singleton.stock_hist_data处理异常：{e}")
        
        # 步骤6: 检查结果并存储
        if not _data:
            # 如果没有任何数据，设置为None
            self.data = None
        else:
            # 存储获取的数据
            self.data = _data

    def get_data(self):
        """
        获取股票历史数据字典
        
        返回值：
            dict: 历史数据字典
                - 键：(date, code, name) 元组
                - 值：pandas.DataFrame，该股票的历史K线数据
                
        DataFrame结构示例：
            date        open   high   low    close  volume    ...
            2021-01-01  10.0   10.5   9.8    10.2   1000000   ...
            2021-01-02  10.2   10.8   10.1   10.5   1200000   ...
            ...
            
        使用示例：
            hist_data = stock_hist_data(date='2024-01-01').get_data()
            
            # 遍历所有股票
            for (date, code, name), df in hist_data.items():
                print(f"{name}({code})的历史数据：")
                print(f"数据长度：{len(df)}天")
                print(f"最新收盘价：{df['close'].iloc[-1]}")
        """
        return self.data


"""
===========================================
股票数据单例模块使用总结（给Python新手）
===========================================

1. 单例模式的概念
   - 确保一个类只有一个实例
   - 多次创建返回同一个对象
   - 数据共享，节省内存

2. 两个单例类
   - stock_data：当天股票快照数据
   - stock_hist_data：股票历史K线数据

3. 使用场景
   - 计算技术指标：需要历史K线数据
   - K线形态识别：需要历史K线数据
   - 策略选股：需要当天数据和历史数据
   - Web展示：需要当天数据

4. 串行执行概念
   - 顺序执行：逐个处理股票数据
   - 避免反爬：每次请求前延迟9-15秒
   - 稳定可靠：虽然慢，但不会被封禁

5. 性能优化
   - 单例模式：避免重复加载数据
   - 串行执行：稳定可靠，避免反爬封禁
   - 缓存机制：历史数据可以缓存到本地文件

6. 注意事项
   - 数据量较大，注意内存使用
   - 网络请求可能失败，有异常处理
   - 线程数不是越多越好，要根据实际情况调整
"""
