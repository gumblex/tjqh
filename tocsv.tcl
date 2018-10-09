#!/usr/bin/tclsh
set lineNumber 0
set output {110000000000,,北京市
120000000000,,天津市
130000000000,,河北省
140000000000,,山西省
150000000000,,内蒙古自治区
210000000000,,辽宁省
220000000000,,吉林省
230000000000,,黑龙江省
310000000000,,上海市
320000000000,,江苏省
330000000000,,浙江省
340000000000,,安徽省
350000000000,,福建省
360000000000,,江西省
370000000000,,山东省
410000000000,,河南省
420000000000,,湖北省
430000000000,,湖南省
440000000000,,广东省
450000000000,,广西壮族自治区
460000000000,,海南省
500000000000,,重庆市
510000000000,,四川省
520000000000,,贵州省
530000000000,,云南省
540000000000,,西藏自治区
610000000000,,陕西省
620000000000,,甘肃省
630000000000,,青海省
640000000000,,宁夏回族自治区
650000000000,,新疆维吾尔自治区
}
while {[gets stdin line] >= 0} {
    regsub -all "\r\n" $line "\n" line
    regsub -all {<a href='.+?\.html'>} $line "" line
    regsub -all {</a>} $line "" line
    regsub -all {</td><td>} $line "," line
    regsub -all {</tr>} $line "\n" line
    regsub -all {</?td>} $line "" line
    regsub -all {<tr class='[a-z]+tr'>} $line "" line
    regsub -all {(\d{12}),(\D)} $line {\1,,\2} line
    regsub -all "\n\n" $line "\n" line
    append output $line
}
set lines [split $output "\n"]
set lines [lsearch -all -inline -not -exact $lines ""]
set lines [lsort -ascii $lines]
puts "统计用区划代码,城乡分类代码,名称"
puts [join $lines "\n"]
