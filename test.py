
import requests

url = "http://push2.eastmoney.com/api/qt/clist/get"
params = {
    'pn': 1,   # 页码
    'pz': 5000, # 每页数量，可以设置较大值一次性获取
    'fs': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23',  # 市场类型，例如：沪市A股、深市A股等[citation:5][citation:7]
    'fields': 'f12,f14,f2,f3,f4,f5,f6,f9,f20,f23'  # 返回字段，例如：代码、名称、最新价、涨跌幅等
}
response = requests.get(url, params=params)
data = response.json()
print(data)