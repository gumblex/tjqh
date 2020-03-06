#!/usr/bin/tclsh
package require http
package require tdom
package require sqlite3
set ::http::defaultCharset cp936

set startyear 2009
set endyear 2019
set baseurl {http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/}

# $year/index.html
sqlite3 db {tjqh.db}
db eval {BEGIN}
db eval {CREATE TABLE IF NOT EXISTS links(url TEXT PRIMARY KEY, status TEXT)}
db eval {CREATE TABLE IF NOT EXISTS tjqh
    (year INTEGER, code INTEGER, category TEXT, name TEXT, PRIMARY KEY(year, code))}

for {set year $startyear} {$year <= $endyear} {incr year} {
    set path "$year/index.html"
    db eval {INSERT OR IGNORE INTO links VALUES ($path, null)}
}
db eval {COMMIT}

proc mergeurl {base new} {
    set path [lrange [split $base /] 0 end-1]
    lappend path $new
    return [join $path /]
}

set trclass {citytr countytr towntr}

set urlpath /
while {$urlpath ne ""} {
    set urlpath [db eval {
        SELECT url FROM links WHERE status IS NULL ORDER BY random() LIMIT 1
    }]
    set year [lindex [split $urlpath /] 0]
    set token [::http::geturl $baseurl$urlpath -timeout 5000]
    set status [::http::status $token]
    if {$status ne "ok"} {
        puts "$status $urlpath"
        ::http::cleanup $token
        continue
    }
    set ncode [::http::ncode $token]
    if {$ncode != 200} {
        puts "$status $urlpath"
        ::http::cleanup $token
        continue
    }
    set data [::http::data $token]
    #set data [encoding convertfrom gb18030 [encoding convertto iso8859-1]]
    ::http::cleanup $token
    set doc [dom parse -html $data]
    set nodetype ""
    db eval {BEGIN}
    foreach tr [domDoc $doc getElementsByTagName tr] {
        set trtype [domNode $tr getAttribute class ""]
        if {$trtype eq ""} {
            continue
        } elseif {$trtype eq "provincetr"} {
            foreach a [domNode $tr getElementsByTagName a] {
                set href [domNode $a getAttribute href ""]
                regexp -- {(\d+)\.html} $href _ match
                set code [lindex $match 0]
                if {$code eq ""} {continue}
                set code [format "%s0000000000" $code] 
                set name [string trim [domNode $a asText]]
                db eval {INSERT OR IGNORE INTO tjqh VALUES ($year,$code,'',$name)}
                set newurl [mergeurl $urlpath $href]
                db eval {INSERT OR IGNORE INTO links(url) VALUES ($newurl)}
            }
        } elseif {[lsearch $trclass $trtype] > -1} {
            set columns {}
            foreach td [domNode $tr getElementsByTagName td] {
                foreach a [domNode $td getElementsByTagName a] {
                    set href [domNode $a getAttribute href ""]
                    if {![regexp -- {(\d+).*\.html} $href]} {continue}
                    set newurl [mergeurl $urlpath $href]
                    db eval {INSERT OR IGNORE INTO links(url) VALUES ($newurl)}
                }
                lappend columns [string trim [domNode $td asText]]
            }
            set code [lindex $columns 0]
            if {$code eq ""} {continue}
            set name [lindex $columns 1]
            db eval {INSERT OR IGNORE INTO tjqh VALUES ($year,$code,'',$name)}
        } elseif {$trtype eq "villagetr"} {
            set columns {}
            foreach td [domNode $tr getElementsByTagName td] {
                lappend columns [string trim [domNode $td asText]]
            }
            set code [lindex $columns 0]
            if {$code eq ""} {continue}
            set category [lindex $columns 1]
            set name [lindex $columns 2]
            db eval {INSERT OR IGNORE INTO tjqh VALUES ($year,$code,$category,$name)}
        } else {continue}
        set nodetype $trtype
    }
    $doc delete
    if {$nodetype ne ""} {
        db eval {UPDATE links SET status=$nodetype WHERE url=$urlpath}
    }
    db eval {COMMIT}
    puts "$nodetype $urlpath"
}

if {![file isdirectory data]} {file mkdir data}

for {set year $startyear} {$year <= $endyear} {incr year} {
    set filename "data/$year.csv"
    puts "Writing $filename"
    set fp [open $filename w]
    puts $fp "统计用区划代码,城乡分类代码,名称"
    db eval {
        SELECT code, category, name FROM tjqh WHERE year=$year
        ORDER BY year, code
    } values {
        puts "$values(code),$values(category),$values(name)"
    }
    close $fp
}
