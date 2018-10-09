#!/bin/bash
mkdir data
for year in $(seq 2009 2017); do
wget --mirror --page-requisites --adjust-extension --no-parent --convert-links --directory-prefix=statgov_tjqh http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/$year/index.html
find statgov_tjqh/www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/$year -name '*.html' -exec env LC_ALL=C grep -a "<tr class='\(city\|county\|town\|village\)tr'>" {} \; | iconv -f gb18030 | tclsh tocsv.tcl > data/$year.csv
done
