# 国家统计局统计用区划代码

爬取[统计局数据](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/)，精确到乡镇街道。已获取的数据在 data 目录。

2022年更新，有 **2009-2021** 年所有历史数据，并修复了已知的缺字、PUA编码增补字符。

tjqh\_getdata.py 依赖：BeautifulSoup4, html5lib, httpx 或 requests。
