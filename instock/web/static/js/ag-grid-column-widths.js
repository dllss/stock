/**
 * AG Grid 列宽配置文件
 * ====================
 * 
 * 功能说明：
 * 为所有表格字段提供列宽配置，避免在 HTML 模板中硬编码
 * 
 * 列宽计算公式：
 * width = max(data_length, min(header_length, 6)) * 14px + 20px padding
 * 
 * 配置结构：
 * 按表名分组，每个表有自己的字段配置
 * 查找顺序：当前表配置 → _global 全局配置 → 默认配置
 * 
 * 使用方式：
 * 在 stock_web_aggrid.html 中引入此文件
 * const tableConfig = columnWidths[nameParam] || {};
 * const widthConfig = tableConfig[col.value] || getDefaultWidthConfig();
 */

// 获取列宽配置对象
function getColumnWidthConfig() {
    return {
        // ============================================
        // cn_stock_spot (每日股票数据) - 41 columns
        // ============================================
        'cn_stock_spot': {
            'date': { min: 100, max: 130, default: 115 },           // 日期
            'code': { min: 85, max: 110, default: 98 },             // 代码
            'name': { min: 85, max: 110, default: 98 },             // 名称
            'new_price': { min: 85, max: 110, default: 98 },        // 最新价(元)
            'change_rate': { min: 100, max: 130, default: 115 },    // 涨跌幅(%)
            'ups_downs': { min: 85, max: 110, default: 98 },        // 涨跌额(元)
            'volume': { min: 110, max: 140, default: 125 },         // 成交量(手)
            'deal_amount': { min: 115, max: 145, default: 130 },    // 成交额(元)
            'amplitude': { min: 95, max: 120, default: 108 },       // 振幅(%)
            'turnoverrate': { min: 105, max: 135, default: 120 },   // 换手率(%)
            'volume_ratio': { min: 80, max: 105, default: 93 },     // 量比
            'open_price': { min: 85, max: 110, default: 98 },       // 今开(元)
            'high_price': { min: 85, max: 110, default: 98 },       // 最高(元)
            'low_price': { min: 85, max: 110, default: 98 },        // 最低(元)
            'pre_close_price': { min: 85, max: 110, default: 98 },  // 昨收(元)
            'speed_increase': { min: 95, max: 120, default: 108 },  // 涨速(%)
            'speed_increase_5': { min: 110, max: 140, default: 125 },   // 5分钟涨跌(%)
            'speed_increase_60': { min: 120, max: 150, default: 135 },  // 60日涨跌幅(%)
            'speed_increase_all': { min: 135, max: 170, default: 153 }, // 年初至今涨跌幅(%)
            'dtsyl': { min: 105, max: 135, default: 120 },          // 市盈率(动)
            'pe9': { min: 100, max: 125, default: 113 },            // 市盈率(TTM)
            'pe': { min: 100, max: 125, default: 113 },             // 市盈率(静)
            'pbnewmrq': { min: 85, max: 110, default: 98 },         // 市净率
            'basic_eps': { min: 120, max: 150, default: 135 },      // 每股收益(元)
            'bvps': { min: 105, max: 135, default: 120 },           // 每股净资产(元)
            'per_capital_reserve': { min: 115, max: 145, default: 130 },    // 每股公积金(元)
            'per_unassign_profit': { min: 110, max: 140, default: 125 },    // 每股未分配利润(元)
            'roe_weight': { min: 135, max: 170, default: 153 },     // 加权净资产收益率(%)
            'sale_gpr': { min: 100, max: 125, default: 113 },       // 毛利率(%)
            'debt_asset_ratio': { min: 115, max: 145, default: 130 },   // 资产负债率(%)
            'total_operate_income': { min: 130, max: 160, default: 145 },   // 营业收入(元)
            'toi_yoy_ratio': { min: 135, max: 170, default: 153 },  // 营业收入同比增长(%)
            'parent_netprofit': { min: 130, max: 160, default: 145 },   // 归属净利润(元)
            'netprofit_yoy_ratio': { min: 145, max: 180, default: 163 },  // 归属净利润同比增长(%)
            'report_date': { min: 100, max: 130, default: 115 },    // 报告期
            'total_shares': { min: 120, max: 150, default: 135 },   // 总股本(股)
            'free_shares': { min: 130, max: 160, default: 145 },    // 已流通股份(股)
            'total_market_cap': { min: 130, max: 160, default: 145 },   // 总市值(元)
            'free_cap': { min: 130, max: 160, default: 145 },       // 流通市值(元)
            'industry': { min: 85, max: 110, default: 98 },         // 所处行业
            'listing_date': { min: 100, max: 130, default: 115 },   // 上市时间
        },
        
        // ============================================
        // cn_stock_chip_race_open/end (早盘/尾盘抢筹数据) - 14 columns
        // ============================================
        'cn_stock_chip_race_open': {
            'bid_rate': { min: 90, max: 115, default: 103 },            // 委比(%)
            'bid_trust_amount': { min: 115, max: 145, default: 130 },   // 委托买入金额(元)
            'bid_deal_amount': { min: 115, max: 145, default: 130 },    // 委托成交金额(元)
            'bid_ratio': { min: 90, max: 115, default: 103 },           // 委托比例(%)
            'limitup_day': { min: 70, max: 90, default: 80 },           // 涨停天数
            'limitup_board': { min: 70, max: 90, default: 80 },         // 涨停板
        },
        'cn_stock_chip_race_end': {
            'bid_rate': { min: 90, max: 115, default: 103 },            // 委比(%)
            'bid_trust_amount': { min: 115, max: 145, default: 130 },   // 委托买入金额(元)
            'bid_deal_amount': { min: 115, max: 145, default: 130 },    // 委托成交金额(元)
            'bid_ratio': { min: 90, max: 115, default: 103 },           // 委托比例(%)
            'limitup_day': { min: 70, max: 90, default: 80 },           // 涨停天数
            'limitup_board': { min: 70, max: 90, default: 80 },         // 涨停板
        },
        
        // ============================================
        // cn_etf_spot (每日ETF数据) - 15 columns
        // ============================================
        'cn_etf_spot': {
            'code': { min: 85, max: 110, default: 98 },             // 代码
            'name': { min: 150, max: 200, default: 180 },           // 名称（ETF名称较长）
            'new_price': { min: 85, max: 110, default: 98 },        // 最新价(元)
            'change_rate': { min: 100, max: 130, default: 115 },    // 涨跌幅(%)
            'volume': { min: 110, max: 140, default: 125 },         // 成交量(手)
            'deal_amount': { min: 115, max: 145, default: 130 },    // 成交额(元)
            'open_price': { min: 85, max: 110, default: 98 },       // 开盘价(元)
            'high_price': { min: 85, max: 110, default: 98 },       // 最高价(元)
            'low_price': { min: 85, max: 110, default: 98 },        // 最低价(元)
            'pre_close_price': { min: 85, max: 110, default: 98 },  // 昨收(元)
            'turnoverrate': { min: 105, max: 135, default: 120 },   // 换手率(%)
            'total_market_cap': { min: 115, max: 145, default: 130 },   // 总市值(元)
            'free_cap': { min: 115, max: 145, default: 130 }        // 流通市值(元)
        },
        
        // ============================================
        // cn_stock_bonus (股票分红配送) - 19 columns
        // ============================================
        'cn_stock_bonus': {
            'code': { min: 85, max: 110, default: 98 },             // 代码
            'name': { min: 85, max: 110, default: 98 },             // 名称
            'bonusaward_rate': { min: 110, max: 140, default: 125 },        // 现金分红-现金分红比例
            'bonusaward_yield': { min: 105, max: 135, default: 120 },       // 现金分红-股息率
            'convertible_rate': { min: 105, max: 135, default: 120 },       // 送转股份-送转比例
            'convertible_total_rate': { min: 110, max: 140, default: 125 }, // 送转股份-送转总比例
            'convertible_transfer_rate': { min: 105, max: 135, default: 120 },  // 送转股份-转股比例
            'ex_dividend_date': { min: 100, max: 130, default: 115 },       // 除权除息日
            'plan_date': { min: 100, max: 130, default: 115 },              // 预案公告日
            'progress': { min: 110, max: 200, default: 150 },                 // 方案进度
            'record_date': { min: 100, max: 130, default: 115 },            // 股权登记日
        },
        
        // ============================================
        // cn_stock_lhb (股票龙虎榜) - 21 columns
        // ============================================
        'cn_stock_lhb': {
            'date': { min: 100, max: 130, default: 115 },               // 日期
            'code': { min: 85, max: 110, default: 98 },                 // 代码
            'name': { min: 85, max: 110, default: 98 },                 // 名称
            'ranking_date': { min: 100, max: 130, default: 115 },       // 上榜日
            'interpret': { min: 100, max: 200, default: 150 },          // 解读
            'new_price': { min: 85, max: 110, default: 98 },            // 收盘价
            'change_rate': { min: 100, max: 130, default: 115 },        // 涨跌幅(%)
            'net_amount_buy': { min: 115, max: 145, default: 130 },     // 龙虎榜净买额
            'sum_buy': { min: 115, max: 145, default: 130 },            // 龙虎榜买入额
            'sum_sell': { min: 115, max: 145, default: 130 },           // 龙虎榜卖出额
            'lhb_amount': { min: 115, max: 145, default: 130 },         // 龙虎榜成交额
            'market_amount': { min: 115, max: 145, default: 130 },      // 市场总成交额
            'net_amount_rate': { min: 145, max: 180, default: 163 },    // 净买额占总成交比(%)
            'sum_rate': { min: 145, max: 180, default: 163 },           // 成交额占总成交比(%)
            'turnoverrate': { min: 105, max: 135, default: 120 },       // 换手率(%)
            'free_cap': { min: 115, max: 145, default: 130 },           // 流通市值
            'reason': { min: 200, max: 800, default: 400 },             // 上榜原因
            'ranking_after_1': { min: 135, max: 170, default: 153 },    // 上榜后1日涨跌幅(%)
            'ranking_after_2': { min: 135, max: 170, default: 153 },    // 上榜后2日涨跌幅(%)
            'ranking_after_5': { min: 135, max: 170, default: 153 },    // 上榜后5日涨跌幅(%)
            'ranking_after_10': { min: 140, max: 175, default: 158 }    // 上榜后10日涨跌幅(%)
        },
        
        // ============================================
        // cn_stock_limitup_reason (涨停原因揭密) - 13 columns
        // ============================================
        'cn_stock_limitup_reason': {
            'date': { min: 100, max: 130, default: 115 },               // 日期
            'code': { min: 85, max: 110, default: 98 },                 // 代码
            'name': { min: 85, max: 110, default: 98 },                 // 名称
            'title': { min: 50, max: 300, default: 100 },              // 原因（简短原因）
            'reason': { min: 200, max: 1000, default: 800 },             // 详因（详细原因）
            'new_price': { min: 85, max: 110, default: 98 },            // 最新价(元)
            'change_rate': { min: 100, max: 130, default: 115 },        // 涨跌幅(%)
            'ups_downs': { min: 85, max: 110, default: 98 },            // 涨跌额(元)
            'turnoverrate': { min: 105, max: 135, default: 120 },       // 换手率(%)
            'volume': { min: 110, max: 140, default: 125 },             // 成交量(手)
            'deal_amount': { min: 115, max: 145, default: 130 },        // 成交额(元)
            'dde': { min: 90, max: 115, default: 103 }                  // DDE
        },
        
        // ============================================
        // cn_stock_fund_flow_concept (概念资金流向) & cn_stock_fund_flow_industry (行业资金流向) - 34 columns
        // ============================================
        'cn_stock_fund_flow_concept': {
            // 今日数据
            'name': { min: 100, max: 130, default: 115 },               // 名称
            'change_rate': { min: 105, max: 135, default: 120 },        // 今日涨跌幅(%)
            'fund_amount': { min: 115, max: 145, default: 130 },        // 今日主力净流入-净额(元)
            'fund_rate': { min: 100, max: 200, default: 150 },          // 今日主力净流入-净占比(%)
            'fund_amount_super': { min: 115, max: 145, default: 130 },  // 今日超大单净流入-净额(元)
            'fund_rate_super': { min: 100, max: 200, default: 150 },    // 今日超大单净流入-净占比(%)
            'fund_amount_large': { min: 115, max: 145, default: 130 },  // 今日大单净流入-净额(元)
            'fund_rate_large': { min: 100, max: 200, default: 150 },    // 今日大单净流入-净占比(%)
            'fund_amount_medium': { min: 115, max: 145, default: 130 }, // 今日中单净流入-净额(元)
            'fund_rate_medium': { min: 100, max: 200, default: 150 },   // 今日中单净流入-净占比(%)
            'fund_amount_small': { min: 115, max: 145, default: 130 },  // 今日小单净流入-净额(元)
            'fund_rate_small': { min: 100, max: 200, default: 150 },    // 今日小单净流入-净占比(%)
            'stock_name': { min: 130, max: 160, default: 145 },         // 今日主力净流入最大股
            
            // 5日数据
            'change_rate_5': { min: 105, max: 135, default: 120 },      // 5日涨跌幅(%)
            'fund_amount_5': { min: 115, max: 145, default: 130 },      // 5日主力净流入-净额(元)
            'fund_rate_5': { min: 100, max: 200, default: 150 },        // 5日主力净流入-净占比(%)
            'fund_amount_super_5': { min: 115, max: 145, default: 130 },// 5日超大单净流入-净额(元)
            'fund_rate_super_5': { min: 100, max: 200, default: 150 },  // 5日超大单净流入-净占比(%)
            'fund_amount_large_5': { min: 115, max: 145, default: 130 },// 5日大单净流入-净额(元)
            'fund_rate_large_5': { min: 100, max: 200, default: 150 },  // 5日大单净流入-净占比(%)
            'fund_amount_medium_5': { min: 115, max: 145, default: 130 },// 5日中单净流入-净额(元)
            'fund_rate_medium_5': { min: 100, max: 200, default: 150 }, // 5日中单净流入-净占比(%)
            'fund_amount_small_5': { min: 115, max: 145, default: 130 },// 5日小单净流入-净额(元)
            'fund_rate_small_5': { min: 100, max: 200, default: 150 },  // 5日小单净流入-净占比(%)
            'stock_name_5': { min: 130, max: 160, default: 145 },       // 5日主力净流入最大股
            
            // 10日数据
            'change_rate_10': { min: 105, max: 135, default: 120 },     // 10日涨跌幅(%)
            'fund_amount_10': { min: 115, max: 145, default: 130 },     // 10日主力净流入-净额(元)
            'fund_rate_10': { min: 100, max: 200, default: 150 },       // 10日主力净流入-净占比(%)
            'fund_amount_super_10': { min: 115, max: 145, default: 130 },// 10日超大单净流入-净额(元)
            'fund_rate_super_10': { min: 100, max: 200, default: 150 }, // 10日超大单净流入-净占比(%)
            'fund_amount_large_10': { min: 115, max: 145, default: 130 },// 10日大单净流入-净额(元)
            'fund_rate_large_10': { min: 100, max: 200, default: 150 }, // 10日大单净流入-净占比(%)
            'fund_amount_medium_10': { min: 115, max: 145, default: 130 },// 10日中单净流入-净额(元)
            'fund_rate_medium_10': { min: 100, max: 200, default: 150 },// 10日中单净流入-净占比(%)
            'fund_amount_small_10': { min: 115, max: 145, default: 130 },// 10日小单净流入-净额(元)
            'fund_rate_small_10': { min: 100, max: 200, default: 150 }, // 10日小单净流入-净占比(%)
            'stock_name_10': { min: 130, max: 160, default: 145 }       // 10日主力净流入最大股
        },
        
        // ============================================
        // cn_stock_fund_flow_industry (行业资金流向) - 34 columns (与概念资金流向结构相同)
        // ============================================
        'cn_stock_fund_flow_industry': {
            // 今日数据
            'name': { min: 100, max: 130, default: 115 },               // 名称
            'change_rate': { min: 105, max: 135, default: 120 },        // 今日涨跌幅(%)
            'fund_amount': { min: 115, max: 145, default: 130 },        // 今日主力净流入-净额(元)
            'fund_rate': { min: 100, max: 200, default: 150 },          // 今日主力净流入-净占比(%)
            'fund_amount_super': { min: 115, max: 145, default: 130 },  // 今日超大单净流入-净额(元)
            'fund_rate_super': { min: 100, max: 200, default: 150 },    // 今日超大单净流入-净占比(%)
            'fund_amount_large': { min: 115, max: 145, default: 130 },  // 今日大单净流入-净额(元)
            'fund_rate_large': { min: 100, max: 200, default: 150 },    // 今日大单净流入-净占比(%)
            'fund_amount_medium': { min: 115, max: 145, default: 130 }, // 今日中单净流入-净额(元)
            'fund_rate_medium': { min: 100, max: 200, default: 150 },   // 今日中单净流入-净占比(%)
            'fund_amount_small': { min: 115, max: 145, default: 130 },  // 今日小单净流入-净额(元)
            'fund_rate_small': { min: 100, max: 200, default: 150 },    // 今日小单净流入-净占比(%)
            'stock_name': { min: 130, max: 160, default: 145 },         // 今日主力净流入最大股
            
            // 5日数据
            'change_rate_5': { min: 105, max: 135, default: 120 },      // 5日涨跌幅(%)
            'fund_amount_5': { min: 115, max: 145, default: 130 },      // 5日主力净流入-净额(元)
            'fund_rate_5': { min: 100, max: 200, default: 150 },        // 5日主力净流入-净占比(%)
            'fund_amount_super_5': { min: 115, max: 145, default: 130 },// 5日超大单净流入-净额(元)
            'fund_rate_super_5': { min: 100, max: 200, default: 150 },  // 5日超大单净流入-净占比(%)
            'fund_amount_large_5': { min: 115, max: 145, default: 130 },// 5日大单净流入-净额(元)
            'fund_rate_large_5': { min: 100, max: 200, default: 150 },  // 5日大单净流入-净占比(%)
            'fund_amount_medium_5': { min: 115, max: 145, default: 130 },// 5日中单净流入-净额(元)
            'fund_rate_medium_5': { min: 100, max: 200, default: 150 }, // 5日中单净流入-净占比(%)
            'fund_amount_small_5': { min: 115, max: 145, default: 130 },// 5日小单净流入-净额(元)
            'fund_rate_small_5': { min: 100, max: 200, default: 150 },  // 5日小单净流入-净占比(%)
            'stock_name_5': { min: 130, max: 160, default: 145 },       // 5日主力净流入最大股
            
            // 10日数据
            'change_rate_10': { min: 105, max: 135, default: 120 },     // 10日涨跌幅(%)
            'fund_amount_10': { min: 115, max: 145, default: 130 },     // 10日主力净流入-净额(元)
            'fund_rate_10': { min: 100, max: 200, default: 150 },       // 10日主力净流入-净占比(%)
            'fund_amount_super_10': { min: 115, max: 145, default: 130 },// 10日超大单净流入-净额(元)
            'fund_rate_super_10': { min: 100, max: 200, default: 150 }, // 10日超大单净流入-净占比(%)
            'fund_amount_large_10': { min: 115, max: 145, default: 130 },// 10日大单净流入-净额(元)
            'fund_rate_large_10': { min: 100, max: 200, default: 150 }, // 10日大单净流入-净占比(%)
            'fund_amount_medium_10': { min: 115, max: 145, default: 130 },// 10日中单净流入-净额(元)
            'fund_rate_medium_10': { min: 100, max: 200, default: 150 },// 10日中单净流入-净占比(%)
            'fund_amount_small_10': { min: 115, max: 145, default: 130 },// 10日小单净流入-净额(元)
            'fund_rate_small_10': { min: 100, max: 200, default: 150 }, // 10日小单净流入-净占比(%)
            'stock_name_10': { min: 130, max: 160, default: 145 }       // 10日主力净流入最大股
        },
        
        // ============================================
        // cn_stock_fund_flow (股票资金流向) - 52 columns
        // ============================================
        'cn_stock_fund_flow': {
            // 基础字段
            'code': { min: 85, max: 110, default: 98 },                 // 代码
            'name': { min: 85, max: 110, default: 98 },                 // 名称
            'new_price': { min: 85, max: 110, default: 98 },            // 最新价
            
            // 今日数据
            'change_rate': { min: 105, max: 135, default: 120 },        // 今日涨跌幅(%)
            'fund_amount': { min: 115, max: 145, default: 130 },        // 今日主力净流入-净额(元)
            'fund_rate': { min: 100, max: 200, default: 150 },          // 今日主力净流入-净占比(%)
            'fund_amount_super': { min: 115, max: 145, default: 130 },  // 今日超大单净流入-净额(元)
            'fund_rate_super': { min: 100, max: 200, default: 150 },    // 今日超大单净流入-净占比(%)
            'fund_amount_large': { min: 115, max: 145, default: 130 },  // 今日大单净流入-净额(元)
            'fund_rate_large': { min: 100, max: 200, default: 150 },    // 今日大单净流入-净占比(%)
            'fund_amount_medium': { min: 115, max: 145, default: 130 }, // 今日中单净流入-净额(元)
            'fund_rate_medium': { min: 100, max: 200, default: 150 },   // 今日中单净流入-净占比(%)
            'fund_amount_small': { min: 115, max: 145, default: 130 },  // 今日小单净流入-净额(元)
            'fund_rate_small': { min: 100, max: 200, default: 150 },    // 今日小单净流入-净占比(%)
            
            // 3日数据
            'change_rate_3': { min: 105, max: 135, default: 120 },      // 3日涨跌幅(%)
            'fund_amount_3': { min: 115, max: 145, default: 130 },      // 3日主力净流入-净额(元)
            'fund_rate_3': { min: 115, max: 145, default: 130 },        // 3日主力净流入-净占比(%)
            'fund_amount_super_3': { min: 115, max: 145, default: 130 },// 3日超大单净流入-净额(元)
            'fund_rate_super_3': { min: 100, max: 200, default: 150 },  // 3日超大单净流入-净占比(%)
            'fund_amount_large_3': { min: 115, max: 145, default: 130 },// 3日大单净流入-净额(元)
            'fund_rate_large_3': { min: 115, max: 145, default: 130 },  // 3日大单净流入-净占比(%)
            'fund_amount_medium_3': { min: 115, max: 145, default: 130 },// 3日中单净流入-净额(元)
            'fund_rate_medium_3': { min: 115, max: 145, default: 130 }, // 3日中单净流入-净占比(%)
            'fund_amount_small_3': { min: 115, max: 145, default: 130 },// 3日小单净流入-净额(元)
            'fund_rate_small_3': { min: 115, max: 145, default: 130 },  // 3日小单净流入-净占比(%)
            
            // 5日数据
            'change_rate_5': { min: 105, max: 135, default: 120 },      // 5日涨跌幅(%)
            'fund_amount_5': { min: 115, max: 145, default: 130 },      // 5日主力净流入-净额(元)
            'fund_rate_5': { min: 100, max: 200, default: 150 },        // 5日主力净流入-净占比(%)
            'fund_amount_super_5': { min: 115, max: 145, default: 130 },// 5日超大单净流入-净额(元)
            'fund_rate_super_5': { min: 100, max: 200, default: 150 },  // 5日超大单净流入-净占比(%)
            'fund_amount_large_5': { min: 115, max: 145, default: 130 },// 5日大单净流入-净额(元)
            'fund_rate_large_5': { min: 100, max: 200, default: 150 },  // 5日大单净流入-净占比(%)
            'fund_amount_medium_5': { min: 115, max: 145, default: 130 },// 5日中单净流入-净额(元)
            'fund_rate_medium_5': { min: 100, max: 200, default: 150 }, // 5日中单净流入-净占比(%)
            'fund_amount_small_5': { min: 115, max: 145, default: 130 },// 5日小单净流入-净额(元)
            'fund_rate_small_5': { min: 100, max: 200, default: 150 },  // 5日小单净流入-净占比(%)
            
            // 10日数据
            'change_rate_10': { min: 105, max: 135, default: 120 },     // 10日涨跌幅(%)
            'fund_amount_10': { min: 115, max: 145, default: 130 },     // 10日主力净流入-净额(元)
            'fund_rate_10': { min: 100, max: 200, default: 150 },       // 10日主力净流入-净占比(%)
            'fund_amount_super_10': { min: 115, max: 145, default: 130 },// 10日超大单净流入-净额(元)
            'fund_rate_super_10': { min: 100, max: 200, default: 150 }, // 10日超大单净流入-净占比(%)
            'fund_amount_large_10': { min: 115, max: 145, default: 130 },// 10日大单净流入-净额(元)
            'fund_rate_large_10': { min: 100, max: 200, default: 150 }, // 10日大单净流入-净占比(%)
            'fund_amount_medium_10': { min: 115, max: 145, default: 130 },// 10日中单净流入-净额(元)
            'fund_rate_medium_10': { min: 100, max: 200, default: 150 },// 10日中单净流入-净占比(%)
            'fund_amount_small_10': { min: 115, max: 145, default: 130 },// 10日小单净流入-净额(元)
            'fund_rate_small_10': { min: 100, max: 200, default: 150 }, // 10日小单净流入-净占比(%)
        },
        
        // ============================================
        // _global 全局配置 (所有表共用的字段)
        // ============================================
        '_global': {
            'date': { min: 100, max: 130, default: 115 },               // 日期
            'datetime': { min: 100, max: 130, default: 115 },           // 日期时间
            'stock_name': { min: 90, max: 115, default: 103 },          // 股票名称
            'stock_name_5': { min: 90, max: 115, default: 103 },        // 股票名称(5日)
            'stock_name_10': { min: 90, max: 115, default: 103 }        // 股票名称(10日)
        }
    };
}

// 获取默认列宽配置
function getDefaultWidthConfig() {
    return { min: 100, max: 200, default: 150 };
}
