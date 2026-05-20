"""清理测试数据"""
import instock.lib.database as mdb

# 删除2024年1月的测试数据
sql = """DELETE FROM cn_stock_spot 
         WHERE code = '600519' 
         AND date BETWEEN '2024-01-02' AND '2024-01-10'"""

try:
    mdb.executeSql(sql)
    print("✅ 清理完成")
except Exception as e:
    print(f"❌ 错误: {e}")
