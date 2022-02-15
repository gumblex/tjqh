#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import sqlite3
import urllib.parse

import bs4
try:
    from httpx import Client
except ImportError:
    from requests import Session as Client


BASEURL = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/'
SESSION = Client()

re_href = re.compile(r'(\d+).*\.html')


PUA_MAPPING = str.maketrans({
# GBK
'\uE78D': '\uFE10', '\uE78E': '\uFE12', '\uE78F': '\uFE11', '\uE790': '\uFE13',
'\uE791': '\uFE14', '\uE792': '\uFE15', '\uE793': '\uFE16', '\uE794': '\uFE17',
'\uE795': '\uFE18', '\uE796': '\uFE19', '\uE7C7': '\u1E3F', '\uE7C8': '\u01F9',
'\uE7E7': '\u303E', '\uE7E8': '\u2FF0', '\uE7E9': '\u2FF1', '\uE7EA': '\u2FF2',
'\uE7EB': '\u2FF3', '\uE7EC': '\u2FF4', '\uE7ED': '\u2FF5', '\uE7EE': '\u2FF6',
'\uE7EF': '\u2FF7', '\uE7F0': '\u2FF8', '\uE7F1': '\u2FF9', '\uE7F2': '\u2FFA',
'\uE7F3': '\u2FFB', '\uE815': '\u2E81', '\uE816': '\U00020087', '\uE817':
'\U00020089', '\uE818': '\U000200CC', '\uE819': '\u2E84', '\uE81A': '\u3473',
'\uE81B': '\u3447', '\uE81C': '\u2E88', '\uE81D': '\u2E8B', '\uE81E': '\u9FB4',
'\uE81F': '\u359E', '\uE820': '\u361A', '\uE821': '\u360E', '\uE822': '\u2E8C',
'\uE823': '\u2E97', '\uE824': '\u396E', '\uE825': '\u3918', '\uE826': '\u9FB5',
'\uE827': '\u39CF', '\uE828': '\u39DF', '\uE829': '\u3A73', '\uE82A': '\u39D0',
'\uE82B': '\u9FB6', '\uE82C': '\u9FB7', '\uE82D': '\u3B4E', '\uE82E': '\u3C6E',
'\uE82F': '\u3CE0', '\uE830': '\u2EA7', '\uE831': '\U000215D7', '\uE832':
'\u9FB8', '\uE833': '\u2EAA', '\uE834': '\u4056', '\uE835': '\u415F', '\uE836':
'\u2EAE', '\uE837': '\u4337', '\uE838': '\u2EB3', '\uE839': '\u2EB6', '\uE83A':
'\u2EB7', '\uE83B': '\U0002298F', '\uE83C': '\u43B1', '\uE83D': '\u43AC',
'\uE83E': '\u2EBB', '\uE83F': '\u43DD', '\uE840': '\u44D6', '\uE841': '\u4661',
'\uE842': '\u464C', '\uE843': '\u9FB9', '\uE844': '\u4723', '\uE845': '\u4729',
'\uE846': '\u477C', '\uE847': '\u478D', '\uE848': '\u2ECA', '\uE849': '\u4947',
'\uE84A': '\u497A', '\uE84B': '\u497D', '\uE84C': '\u4982', '\uE84D': '\u4983',
'\uE84E': '\u4985', '\uE84F': '\u4986', '\uE850': '\u499F', '\uE851': '\u499B',
'\uE852': '\u49B7', '\uE853': '\u49B6', '\uE854': '\u9FBA', '\uE855':
'\U000241FE', '\uE856': '\u4CA3', '\uE857': '\u4C9F', '\uE858': '\u4CA0',
'\uE859': '\u4CA1', '\uE85A': '\u4C77', '\uE85B': '\u4CA2', '\uE85C': '\u4D13',
'\uE85D': '\u4D14', '\uE85E': '\u4D15', '\uE85F': '\u4D16', '\uE860': '\u4D17',
'\uE861': '\u4D18', '\uE862': '\u4D19', '\uE863': '\u4DAE', '\uE864': '\u9FBB',

# Sogou
'\uE050': '\U0002E9F5', '\uE051': '\U0002C62D', '\uE052': '\U0002C386',
'\uE053': '\u39D1', '\uE054': '\u40DA', '\uE055': '\u3ED5', '\uE056': '\u3EDE',
'\uE057': '\u365F', '\uE058': '\U0002C35B', '\uE059': '\U0002AC94', '\uE05A':
'\U0002C72F', '\uE05B': '\U0002C542', '\uE05C': '\U0002B7A9', '\uE05D':
'\u3FA6', '\uE05E': '\U0002C029', '\uE05F': '\U0002C364', '\uE060':
'\U0002C02A', '\uE061': '\U0002C613', '\uE062': '\u45EA', '\uE063':
'\U00030858', '\uE064': '\U00031145', '\uE065': '\U0002CB31', '\uE066':
'\U0002028E', '\uE067': '\U00021E47', '\uE068': '\U0002BE29', '\uE069':
'\U00028693', '\uE06A': '\u3E63', '\uE06B': '\U0002C317', '\uE06C':
'\U0002DD0A', '\uE06D': '\U000201D4', '\uE06E': '\U00030D67', '\uE06F':
'\U00030F91', '\uE071': '\U0003084B', '\uE072': '\U0002479E', '\uE073':
'\U00027285', '\uE074': '\U00022489', '\uE075': '\U0002B5E0', '\uE076':
'\u3EE1', '\uE077': '\U00030D5D', '\uE078': '\U00030F6A', '\uE079':
'\U00030D6E', '\uE07B': '\U00020CD0', '\uE07C': '\u3CCC', '\uE07D': '\u3643',
'\uE07E': '\U00030FAB', '\uE07F': '\u3CC7', '\uE082': '\U000291D5', '\uE083':
'\U00020676', '\uE084': '\U0002866E', '\uE085': '\U0002C8D9', '\uE086':
'\u3EA9', '\uE087': '\U00028676', '\uE088': '\U00028678', '\uE089': '\u3CBB',
'\uE08A': '\u386F', '\uE08B': '\U0002C1D5', '\uE08C': '\U000225B3', '\uE08D':
'\U0002C8DE', '\uE08E': '\U00028E1D', '\uE08F': '\u36A4', '\uE090': '\u3EAD',
'\uE091': '\U0002BB5F', '\uE092': '\U0002BB62', '\uE094': '\u449C', '\uE095':
'\U0002B1ED', '\uE096': '\U0002B404', '\uE097': '\u356E', '\uE098':
'\U0002BD77', '\uE099': '\U00028694', '\uE09A': '\U00023C98', '\uE09B':
'\U00023C97', '\uE09C': '\U0002C1D8', '\uE09D': '\U0002C1D9', '\uE09E':
'\U0002C8E1', '\uE09F': '\u48BA', '\uE0A0': '\U0002BC0D', '\uE0A1':
'\U00028695', '\uE0A2': '\U0002B61C', '\uE0A3': '\U0002C618', '\uE0A4':
'\U0002B61D', '\uE0A5': '\U0002B7A5', '\uE0A6': '\U0002AED0', '\uE0A7':
'\u48BC', '\uE0A8': '\U00026B5C', '\uE0A9': '\U0002CA02', '\uE0AA':
'\U0002CB29', '\uE0AB': '\u37C3', '\uE0AC': '\u48BE', '\uE0AD': '\u344A',
'\uE0AE': '\U0002B748', '\uE0AF': '\u344E', '\uE0B0': '\U0002652F', '\uE0B1':
'\U0002CC75', '\uE0B2': '\U0002C8F3', '\uE0B3': '\U0002CA7D', '\uE0B4':
'\U0002CBBF', '\uE0B5': '\U0002CBC0', '\uE0B6': '\u36B0', '\uE0B7': '\u36B4',
'\uE0B8': '\U000216DF', '\uE0B9': '\U0002A970', '\uE0BA': '\U0002CCF5',
'\uE0BB': '\U000299EC', '\uE0BC': '\U0002CCF6', '\uE0BD': '\u4339', '\uE0BE':
'\u36C3', '\uE0BF': '\U00024912', '\uE0C0': '\U00030875', '\uE0C1': '\u3EB9',
'\uE0C2': '\u39E5', '\uE0C3': '\u9FCD', '\uE0C4': '\U0002C72C', '\uE0C5':
'\U00026C21', '\uE0C6': '\u3B55', '\uE0C7': '\u48C5', '\uE0C8': '\U00025430',
'\uE0C9': '\U00020860', '\uE0CA': '\U0002AFA2', '\uE0CB': '\U00021DEB',
'\uE0CC': '\U0002AA30', '\uE0CD': '\U0002B4E7', '\uE0CE': '\U0002CB2C',
'\uE0CF': '\U0002CB2E', '\uE0D0': '\u409C', '\uE0D1': '\U0002CB2D', '\uE0D2':
'\u38DD', '\uE0D3': '\u43E1', '\uE0D4': '\U0002B808', '\uE0D5': '\u3DB2',
'\uE0D6': '\u3CDA', '\uE0D7': '\U0002C8FD', '\uE0D8': '\U0002BC21', '\uE0D9':
'\U0002C621', '\uE0DA': '\U0002CCFD', '\uE0DB': '\U0002C629', '\uE0DC':
'\U0002B127', '\uE0DD': '\u73F9', '\uE0DE': '\U0002493A', '\uE0DF':
'\U0002A7DD', '\uE0E0': '\U00024938', '\uE0E1': '\U0002BB7C', '\uE0E2':
'\U00026BEC', '\uE0E3': '\u44C2', '\uE0E4': '\U0002C0A9', '\uE0E5':
'\U0002B806', '\uE0E6': '\U00028408', '\uE0E7': '\U0002AA36', '\uE0E8':
'\U0002CB38', '\uE0E9': '\U0002CB3B', '\uE0EA': '\U0002CB39', '\uE0EB':
'\U0002CB3F', '\uE0EC': '\U0002CB41', '\uE0ED': '\U00024654', '\uE0EE':
'\U0002B8B8', '\uE0EF': '\U00028213', '\uE0F0': '\U000286ED', '\uE0F1':
'\u810E', '\uE0F2': '\U0002B5E7', '\uE0F3': '\u4368', '\uE0F4': '\u4369',
'\uE0F5': '\U0002C288', '\uE0F6': '\U0002C488', '\uE0F7': '\U00028E99',
'\uE0F8': '\u49D1', '\uE0F9': '\u36D2', '\uE0FA': '\U0002B76B', '\uE0FB':
'\U00021750', '\uE0FC': '\U0002BC30', '\uE0FD': '\u36DA', '\uE0FE':
'\U0002CCFF', '\uE0FF': '\U0002B128', '\uE100': '\U0002C62B', '\uE101':
'\U0002CD00', '\uE102': '\u40AE', '\uE103': '\u3EC9', '\uE104': '\u3ECC',
'\uE105': '\U0002BB83', '\uE106': '\u364D', '\uE107': '\u44EB', '\uE108':
'\u44EC', '\uE109': '\u44E8', '\uE10A': '\u44DB', '\uE10B': '\u9FCE', '\uE10C':
'\U000231B3', '\uE10D': '\u3AF0', '\uE10E': '\U0002CC56', '\uE10F':
'\U0002C7FD', '\uE110': '\u4383', '\uE111': '\U0002B4EF', '\uE112':
'\U0002B7F9', '\uE113': '\U0002B7FC', '\uE114': '\u41DE', '\uE115': '\u4759',
'\uE116': '\u43F2', '\uE117': '\U0002CA8D', '\uE118': '\U0002C28D', '\uE119':
'\U0002C1F9', '\uE11A': '\U0002C361', '\uE11B': '\u3944', '\uE11C':
'\U0002C907', '\uE11D': '\U0002B36F', '\uE11E': '\U0002C90A', '\uE11F':
'\U0002B372', '\uE120': '\U0002CBCE', '\uE121': '\U00030307', '\uE122':
'\u36E5', '\uE123': '\U0002C62C', '\uE124': '\U0002CD02', '\uE125':
'\U0002B626', '\uE126': '\U0002B7C5', '\uE127': '\U0002C62F', '\uE128':
'\U0002B627', '\uE129': '\u3ED3', '\uE12A': '\U0002AEE8', '\uE12B': '\u3ED2',
'\uE12C': '\u3ED4', '\uE12D': '\u3ED1', '\uE12E': '\U0002497F', '\uE12F':
'\U0002497D', '\uE130': '\U00022BFA', '\uE131': '\U0002A8FB', '\uE132':
'\U0002139A', '\uE133': '\U000234C9', '\uE134': '\U0002C0CA', '\uE135':
'\U0002CDD5', '\uE136': '\u48D3', '\uE137': '\U0002B410', '\uE138':
'\U0002CE7C', '\uE139': '\U000251A7', '\uE13A': '\U0002C457', '\uE13B':
'\U00027FF9', '\uE13C': '\U0002B5AE', '\uE13D': '\u9FCF', '\uE13E':
'\U0002CB4A', '\uE13F': '\U00028C47', '\uE140': '\U0002B4F6', '\uE141':
'\U0002CB4E', '\uE142': '\U0002B5AF', '\uE143': '\u3B37', '\uE144': '\u43FD',
'\uE145': '\U0002CC5F', '\uE146': '\U0002CD87', '\uE147': '\U0002B6ED',
'\uE148': '\u3E84', '\uE149': '\U0002BDF7', '\uE14A': '\U0002CBB1', '\uE14B':
'\U0002C2A4', '\uE14C': '\u3D14', '\uE14D': '\u36F1', '\uE14E': '\u36F9',
'\uE14F': '\U0002CD03', '\uE150': '\U0002B628', '\uE151': '\U00031152',
'\uE152': '\u3EDF', '\uE153': '\U000249DB', '\uE154': '\u3EE0', '\uE155':
'\u4A3F', '\uE156': '\u6987', '\uE157': '\U0002CAA9', '\uE158': '\U0002C494',
'\uE159': '\u40C5', '\uE15A': '\U00029401', '\uE15B': '\U0002CA0E', '\uE15C':
'\U0002B413', '\uE15D': '\u48D8', '\uE15E': '\U0002BAC7', '\uE15F': '\u3B08',
'\uE160': '\u3B0A', '\uE161': '\U0002C9AF', '\uE162': '\u3857', '\uE163':
'\U0002B4F9', '\uE164': '\U0002CB56', '\uE165': '\U0002CB5A', '\uE166':
'\U0002CB5B', '\uE167': '\U0002CD8B', '\uE168': '\U0002CD8D', '\uE169':
'\U0002677C', '\uE16A': '\U0002B536', '\uE16B': '\U0002B7A1', '\uE16C':
'\U0002B300', '\uE16D': '\U0002B5B3', '\uE16E': '\U0002B62A', '\uE16F':
'\U0002B62C', '\uE170': '\U00021413', '\uE171': '\U0002A917', '\uE172':
'\U000235CB', '\uE173': '\U0002CE18', '\uE174': '\u40CE', '\uE175':
'\U0002C497', '\uE176': '\U00025532', '\uE177': '\U0002B696', '\uE178':
'\u3B0E', '\uE179': '\U00028C4F', '\uE17A': '\U0002CB63', '\uE17B':
'\U0002CB64', '\uE17C': '\U00025BBE', '\uE17D': '\u3666', '\uE17E':
'\U0002C089', '\uE17F': '\U0002B695', '\uE180': '\U0002CD90', '\uE181':
'\U0002CD8F', '\uE182': '\U00029F7E', '\uE183': '\U0002CE1A', '\uE184':
'\U0002310E', '\uE185': '\u3F4F', '\uE186': '\u3BBE', '\uE187': '\U0002C91D',
'\uE188': '\U0002C642', '\uE189': '\u3EEC', '\uE18A': '\u45D6', '\uE18B':
'\u3807', '\uE18C': '\U0002CB69', '\uE18D': '\U00029F83', '\uE18E': '\u9CA6',
'\uE18F': '\u3D50', '\uE190': '\U0002CE23', '\uE191': '\U000256DA', '\uE192':
'\U0002B37D', '\uE193': '\u3723', '\uE194': '\U0002CD0A', '\uE195':
'\U00024A72', '\uE196': '\u3EF8', '\uE197': '\U0002C391', '\uE198': '\u3EF5',
'\uE199': '\U00024A44', '\uE19A': '\U0002C79F', '\uE19B': '\U000287BC',
'\uE19C': '\U0002B7E6', '\uE19D': '\U0002CE88', '\uE19E': '\U0002B81C',
'\uE19F': '\u3B1A', '\uE1A0': '\u45DB', '\uE1A1': '\U000273A5', '\uE1A2':
'\u3813', '\uE1A3': '\U0002AA58', '\uE1A4': '\U0002CB6C', '\uE1A5':
'\U00028C51', '\uE1A6': '\U0002CB6F', '\uE1A7': '\u422A', '\uE1A8':
'\U0002B5F4', '\uE1A9': '\U0002CE26', '\uE1AA': '\U000228CF', '\uE1AB':
'\U0002B137', '\uE1AC': '\U00024A7D', '\uE1AD': '\u3EFF', '\uE1AE':
'\U0002CB73', '\uE1AF': '\U0002CB76', '\uE1B0': '\U0002B50D', '\uE1B1':
'\U0002CB78', '\uE1B2': '\U00028C54', '\uE1B3': '\U0002CB7C', '\uE1B4':
'\U0002B50E', '\uE1B5': '\U0002CE2A', '\uE1B6': '\U0002CD9F', '\uE1B7':
'\U0002CDA0', '\uE1B8': '\U0002B811', '\uE1B9': '\u46A6', '\uE1BA':
'\U0002CDA8', '\uE1BB': '\U00026221', '\uE1BC': '\U0002B138', '\uE1BD':
'\U00024A8C', '\uE1BE': '\u4C02', '\uE1BF': '\U0002C7C1', '\uE1C0': '\u45F4',
'\uE1C1': '\U00029A81', '\uE1C2': '\U00025CD8', '\uE1C3': '\U0002648D',
'\uE1C4': '\u4396', '\uE1C5': '\u3734', '\uE1C6': '\U0002C64A', '\uE1C7':
'\U0002308F', '\uE1C8': '\u3C00', '\uE1C9': '\U000274BD', '\uE1CA':
'\U000287E0', '\uE1CB': '\U0002CDAE', '\uE1CC': '\U00028B49', '\uE1CD':
'\u3E0C', '\uE1CE': '\U0002C64B', '\uE1CF': '\U00024AC9', '\uE1D0':
'\U0002CE93', '\uE1D1': '\U00023313', '\uE1D2': '\u3B2C', '\uE1D3': '\u4082',
'\uE1D4': '\U000241B5', '\uE1D5': '\U0002A83D', '\uE1D6': '\u356D', '\uE1D7':
'\u3F72', '\uE1D8': '\U00022650', '\uE1D9': '\U0002E266', '\uE1DA':
'\U00021336', '\uE1DB': '\u3635', '\uE1DC': '\U00024B62', '\uE1DE': '\u3B4B',
'\uE1E0': '\U0003064F', '\uE1E1': '\U00028468', '\uE1E2': '\U00025584',
'\uE1E3': '\u9FC3', '\uE1E4': '\U0002B11F', '\uE1E5': '\U0002E26E', '\uE1E6':
'\u413B', '\uE1E7': '\u4D5A', '\uE1E8': '\u394F', '\uE1E9': '\U0002B892',
'\uE1EA': '\U00028C4E', '\uE1EB': '\u3A97', '\uE1EC': '\u4D4D', '\uE1ED':
'\U0002CB3A', '\uE1EE': '\U0002CBA4'
})


def init_db(filename, startyear=2009, endyear=2021):
    db = sqlite3.connect(filename, isolation_level=None)
    db.execute('BEGIN')
    db.execute('CREATE TABLE IF NOT EXISTS links('
        'url TEXT PRIMARY KEY, status TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS tjqh ('
        'year INTEGER, code INTEGER, category TEXT, name TEXT, '
        'PRIMARY KEY(code, year))')
    db.execute('CREATE TABLE IF NOT EXISTS tjqh_original ('
        'year INTEGER, code INTEGER, category TEXT, name TEXT, '
        'PRIMARY KEY(code, year))')
    for year in range(startyear, endyear+1):
        path = '%s/index.html' % year
        db.execute('INSERT OR IGNORE INTO links VALUES (?, null)', (path,))
    db.execute('COMMIT')
    return db


def get_link(db, urlpath):
    cur = db.cursor()
    year = int(urlpath.split('/')[0])
    req = SESSION.get(BASEURL + urlpath, timeout=5)
    if req.status_code != 200:
        print('%s %s' % (req.status_code, urlpath))
        cur.execute('UPDATE links SET status=404 WHERE url=?', (urlpath,))
        return
    encoding = None
    if b'charset=gb' in req.content[:1024].lower():
        encoding = 'gb18030'
    soup = bs4.BeautifulSoup(req.content, 'html5lib', from_encoding=encoding)
    nodetype = None
    cur.execute("BEGIN")
    for tr in soup.find_all('tr'):
        trtype = (tr.get('class') or [''])[0].strip()
        if not trtype:
            continue
        elif trtype == 'provincetr':
            for a in tr.find_all('a'):
                href = a.get('href', '')
                match = re_href.search(href)
                if not match:
                    continue
                code = int(match.group(1) + '0000000000')
                name = a.get_text(strip=True)
                cur.execute("REPLACE INTO tjqh VALUES (?,?,'',?)", (
                    year, code, name))
                newurl = urllib.parse.urljoin(urlpath, href)
                cur.execute("INSERT OR IGNORE INTO links(url) VALUES (?)", (
                    newurl,))
        elif trtype in ('citytr', 'countytr', 'towntr'):
            columns = []
            for td in tr.find_all('td'):
                for a in td.find_all('a'):
                    href = a.get('href', '')
                    if not re_href.search(href):
                        continue
                    newurl = urllib.parse.urljoin(urlpath, href)
                    cur.execute("INSERT OR IGNORE INTO links(url) VALUES (?)", (
                        newurl,))
                columns.append(td.get_text(strip=True))
            if len(columns) < 2 or not columns[0]:
                continue
            cur.execute("REPLACE INTO tjqh VALUES (?,?,'',?)", (
                year, int(columns[0]), columns[1]))
        elif trtype == 'villagetr':
            columns = []
            for td in tr.find_all('td'):
                columns.append(td.get_text(strip=True))
            if len(columns) < 3 or not columns[0]:
                continue
            cur.execute("REPLACE INTO tjqh VALUES (?,?,?,?)", (
                year, int(columns[0]), columns[1], columns[2]))
        elif trtype in ('cityhead', 'countyhead', 'townhead', 'villagehead'):
            trtype = trtype.replace('head', 'tr')
        else:
            continue
        nodetype = trtype
    if nodetype:
        cur.execute("UPDATE links SET status=? WHERE url=?", (nodetype, urlpath))
    cur.execute("COMMIT")
    print(nodetype, urlpath)


def fix_names(db):
    def sqlite_regexp(pattern, string):
        return re.search(pattern, string) is not None

    db.create_function('regexp', 2, sqlite_regexp, deterministic=True)
    db.create_function('regexp_sub', 3, re.sub, deterministic=True)
    cur = db.cursor()
    cur.execute("PRAGMA case_sensitive_like=1")
    cur.execute("PRAGMA cache_size=-1048576")
    cur.execute("BEGIN")
    cur.execute(
        "SELECT year, code, category, name FROM tjqh "
        "WHERE name REGEXP '[\uE000-\uF8FF]'")
    fix_count = 0
    for year, code, category, name in cur.fetchall():
        code = str(code)
        new_name = name.translate(PUA_MAPPING)
        if new_name == name:
            # print("[%s/%s] PUA not changed: %s" % (year, code, name))
            continue
        cur.execute("INSERT OR IGNORE INTO tjqh_original VALUES (?,?,?,?)",
            (year, code, category, name))
        cur.execute("UPDATE tjqh SET name=? WHERE year=? AND code=?",
            (new_name, year, code))
        fix_count += 1
        # print("[%s/%s] PUA: %s -> %s" % (year, code, name, new_name))
    for year, code, name in (
        (2016, 520324116000, '桴㯊镇'),
        (2021, 360729102209, '张公𡌶村委会'),
        (2017, 411224104211, '撞子沟村民委员会'),
        (2017, 411224201225, '壮沟村民委员会'),
        (2019, 500153108200, '大青㭎村村民委员会'),
        (2020, 500153108200, '大青㭎村村民委员会'),
        (2021, 500153108200, '大青㭎村村民委员会'),
    ):
        cur.execute("""
            INSERT OR IGNORE INTO tjqh_original
            SELECT year, code, category, name FROM tjqh
            WHERE year=? AND code=? AND name!=?
        """, (year, code, name))
        cur.execute("UPDATE tjqh SET name=? WHERE year=? AND code=?",
            (name, year, code))
        fix_count += 1
    cur.execute("""
        SELECT t1.year year1, t1.code code1, t1.name name1,
          t2.year year2, t2.code code2, t2.name name2
        FROM tjqh t1
        INNER JOIN tjqh t2 ON t2.year!=t1.year
        AND t2.code = t1.code
        AND t2.name LIKE regexp_sub('([?？\uE000\uFFFD]+)', '%', t1.name)
        AND NOT t2.name REGEXP '[?？\uE000\uFFFD]'
        WHERE t1.name REGEXP '[?？\uE000\uFFFD]'
        ORDER BY (year2>year1), abs(year2-year1)
    """)
    fixed = set()
    for year1, code1, name1, year2, code2, name2 in cur.fetchall():
        if (year1, code1) in fixed:
            continue
        fixed.add((year1, code1))
        cur.execute("""
            INSERT OR IGNORE INTO tjqh_original
            SELECT year, code, category, name FROM tjqh
            WHERE year=? AND code=? AND name!=?
        """, (year1, code1, name2))
        cur.execute("UPDATE tjqh SET name=? WHERE year=? AND code=?",
            (name2, year1, code1))
        fix_count += 1
        # print(year1, code1, name1, year2, code2, name2)
    cur.execute("COMMIT")
    print("Fixed %d names." % fix_count)


def main(dbname, startyear=2009, endyear=2021):
    db = init_db(dbname, startyear, endyear)
    cur = db.cursor()
    while True:
        row = cur.execute("""
            SELECT url FROM links
            WHERE status IS NULL OR status='404' ORDER BY random() LIMIT 1
        """).fetchone()
        if row is None:
            break
        try:
            get_link(db, row[0])
        except Exception as ex:
            print(repr(ex), row[0])

    fix_names(db)

    os.makedirs('data', exist_ok=True)
    for year in range(startyear, endyear+1):
        filename = 'data/%s.csv' % year
        print("Writing " + filename)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("统计用区划代码,城乡分类代码,名称\n")
            for row in cur.execute(
                "SELECT code, category, name FROM tjqh "
                "WHERE year=? ORDER BY year, code", (year,)
            ):
                f.write(','.join(map(str, row)))
                f.write('\n')
    db.close()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        dbname = sys.argv[1]
    else:
        dbname = 'tjqh.db'
    main(dbname)
