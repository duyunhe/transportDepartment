# -*- coding: utf-8 -*-
# @Time    : 2019/8/12 9:58
# @Author  : yhdu@tongwoo.cn
# @简介    : 上传数据缺失严重，检查
# @File    : lack.py


import xlrd
import xlwt
import os
from collections import defaultdict
import cx_Oracle
import codecs
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'


def fetch_excel(filename):
    wb = xlrd.open_workbook('./data/excel/' + filename)
    try:
        sht = wb.sheet_by_name(u'昨日不合格数据')
    except xlrd.biffh.XLRDError:
        print filename, "error"
        return set()

    n = sht.nrows
    veh_set = set()
    for row in range(1, n):
        veh = sht.cell_value(row, 0)
        reason = sht.cell_value(row, 8)
        company = sht.cell_value(row, 4)
        if reason.find(u"缺失") != -1:
            # print veh, reason
            veh = veh.encode('utf-8')
            veh_set.add(veh)
    return veh_set


def fetch_60_8():
    wb = xlrd.open_workbook('./data/excel/60+8.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    veh_set = set()
    for i in range(1, n):
        veh = sht.cell_value(i, 0)
        veh_set.add(veh.encode('utf-8'))
    return veh_set


def add_veh(stat, veh_set):
    for veh in veh_set:
        stat[veh] += 1


def get_15():
    wb = xlrd.open_workbook('./data/sup15.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    set_15 = set()
    for i in range(n):
        veh = sht.cell_value(i, 0)
        # print veh
        set_15.add(veh.encode('utf-8'))
    sht = wb.sheet_by_index(1)
    n = sht.nrows
    # set_15 = set()
    for i in range(n):
        veh = sht.cell_value(i, 0)
        # print veh
        set_15.add(veh.encode('utf-8'))
    print "all data", len(set_15)
    return set_15


def main():
    isu_list = load_txt()
    conn = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur = conn.cursor()
    sql = "select mdt_no, vehi_no from vw_vehicle t"
    cur.execute(sql)
    veh_dict = {}
    for item in cur:
        mdt, veh = item[:]
        veh_dict[mdt] = veh
    set_400 = set()
    no_cnt = 0
    for isu in isu_list:
        try:
            veh = veh_dict[isu]
            set_400.add(veh)
        except KeyError:
            no_cnt += 1

    set_60 = fetch_60_8()
    set_15 = get_15()
    fp = os.listdir('./data/excel/')
    veh_stat = defaultdict(int)
    for f in fp:
        add_veh(veh_stat, fetch_excel(f))
    res = sorted(veh_stat.items(), key=lambda x: x[1], reverse=True)
    check_set = set(veh_stat.keys())
    for info in res:
        veh, cnt = info
        # veh = veh.encode('utf-8')
        if cnt > 0:
            print veh, '\t', cnt
        # if veh in set_60:
        #     print veh
    print len([t for t in res if t[1] > 0])
    #
    print len(check_set & set_400)
    # for veh in check_set - set_15:
    #     print veh, '\t', veh_stat[veh]


def load_txt():
    fp = open("./txt/filterAccOffISU.txt")
    line = fp.readline()
    isu = line.strip('\n').split(',')
    fp.close()
    print "load isu", len(isu)
    return isu


def get_new():
    fp = os.listdir('./data/excel/')
    veh_stat = defaultdict(int)
    for f in fp:
        add_veh(veh_stat, fetch_excel(f))
    res = sorted(veh_stat.items(), key=lambda x: x[1], reverse=True)
    check_set = set(veh_stat.keys())
    return check_set


def load_db():
    isu_list = load_txt()
    conn = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur = conn.cursor()
    sql = "select mdt_no, vehi_no from vw_vehicle t"
    cur.execute(sql)
    veh_dict = {}
    for item in cur:
        mdt, veh = item[:]
        veh_dict[mdt] = veh
    set_400 = set()
    no_cnt = 0
    for isu in isu_list:
        try:
            veh = veh_dict[isu]
            set_400.add(veh)
        except KeyError:
            no_cnt += 1
    # print "no cnt ", no_cnt
    set_15 = get_15()
    hz_veh = get_new()
    print "veh ", len(set_15)
    all_veh = set_15
    wb = xlwt.Workbook(encoding='utf-8')
    a = '浙A0T592' in set_400
    ws = wb.add_sheet('no filter')
    for i, veh in enumerate(all_veh - set_400):
        ws.write(i, 0, veh)
    ws = wb.add_sheet('with filter')
    for i, veh in enumerate(set_400 & all_veh):
        ws.write(i, 0, veh)
    wb.save('./data/test15.xls')
    # with open("./data/4.csv", 'w') as fp:
    #     for veh in set_400 & all_veh:
    #         fp.write("{0}\n".format(veh))
    #     fp.close()
    # with codecs.open("./data/5.csv", 'w') as fp:
    #     for veh in all_veh - set_400:
    #         fp.write("{0}\n".format(veh))
    #     fp.close()
    cur.close()
    conn.close()


def list_veh():
    fp = os.listdir('./data/excel/')
    veh_stat = defaultdict(int)
    for f in fp:
        add_veh(veh_stat, fetch_excel(f))
    set_15 = get_15()
    res = sorted(veh_stat.items(), key=lambda x: x[1], reverse=True)
    res = [[v, c] for v, c in res if c > 1]
    for veh, cnt in res:
        if veh not in set_15:
            print veh, '\t', cnt


def load_city():
    wb = xlrd.open_workbook('./data/city.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    set_city = set()
    for i in range(1, n):
        veh = sht.cell_value(i, 1)
        # print veh
        set_city.add(veh.encode('utf-8'))
    return set_city


def load_sub():
    wb = xlrd.open_workbook('./data/city.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    set_city = set()
    for i in range(1, n):
        veh = sht.cell_value(i, 1)
        # print veh
        set_city.add(veh.encode('utf-8'))
    return set_city


def load_veh():
    isu_list = load_txt()
    conn = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur = conn.cursor()
    sql = "select mdt_no, vehi_no from vw_vehicle t"
    cur.execute(sql)
    veh_dict = {}
    for item in cur:
        mdt, veh = item[:]
        veh_dict[mdt] = veh
    set_400 = set()
    no_cnt = 0
    for isu in isu_list:
        try:
            veh = veh_dict[isu]
            set_400.add(veh)
        except KeyError:
            no_cnt += 1
    # print "no cnt ", no_cnt
    list_1 = ['浙A09527D', '浙A3W877', '浙A8D273', '浙A8D253']
    for veh in list_1:
        if veh in set_400:
            print veh


def load_veh0():
    veh_list = ['浙A9B068', '浙A1B850', '浙AE5577', '浙A6G680', '浙A8C760', '浙A97652']
    return set(veh_list)


def sub():
    wb = xlrd.open_workbook('./data/sup60_8.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    set_60 = set()
    for i in range(n):
        veh = sht.cell_value(i, 0)
        set_60.add(veh.encode('utf-8'))
    city = load_city()
    print len(set_60)
    s = load_veh0()
    rem = set_60 - city
    print len(rem)

    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('1')
    for i, veh in enumerate(rem):
        ws.write(i, 0, veh)
    wb.save('./data/test60_8.xls')


sub()
