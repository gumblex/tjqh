# 国家统计局统计用区划代码

爬取国家统计局《[全国统计用区划代码和城乡划分代码](http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/)》，精确到乡镇街道。已获取的数据在 `data` 目录。

2023年更新，有 **2009-2023** 年所有历史数据，并修复了已知的缺字、PUA编码增补字符。

tjqh\_getdata.py 依赖：BeautifulSoup4, html5lib, httpx 或 requests。
