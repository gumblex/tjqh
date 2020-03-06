#!/bin/bash
mkdir -p data
for year in $(seq 2009 2019); do
if [ -s data/$year.csv ]; then continue; fi
wget -e robots=off --mirror --page-requisites --no-parent --convert-links \
  --tries=10 --retry-on-http-error=400,429,500,502,504 --compression=auto \
  --directory-prefix=statgov_tjqh \
  http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/$year/index.html
find statgov_tjqh/www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/$year \
  -name '*.html' \
  -exec env LC_ALL=C perl -0777 -p -e "s,</td>\s+<,</td><,g" {} \; \
  | env LC_ALL=C sed 's,</tr>,\0\n,g' \
  | env LC_ALL=C grep -a "<tr class='\(city\|county\|town\|village\)tr'>" \
  | iconv -f gb18030 | tclsh tocsv.tcl > data/$year.csv
done
