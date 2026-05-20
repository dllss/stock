"""查询测试结果"""
import instock.lib.database as mdb

sql = """SELECT date, code, name, new_price 
         FROM cn_stock_spot 
         WHERE code = '600519' 
         AND date >= '2024-01-02' 
         AND date <= '2024-01-10' 
         ORDER BY date 
         LIMIT 5"""

try:
    result = mdb.executeSql(sql)
    print("✅ 查询结果：")
    if result:
        for row in result:
            print(row)
    else:
        print("没有数据")
except Exception as e:
    print(f"❌ 错误: {e}")
