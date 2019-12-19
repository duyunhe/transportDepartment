# -*- coding: utf-8 -*-
# @Time    : 2019/8/13 9:24
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : missing_813.py


import xlrd
import cx_Oracle
import os
from datetime import datetime, timedelta
import xlwt
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'


def fetch_60_8():
    wb = xlrd.open_workbook('./data/sup60_8.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    veh_set = set()
    for i in range(1, n):
        veh = sht.cell_value(i, 0)
        veh_set.add(veh.encode('utf-8'))
    return veh_set


def fetch_excel():
    wb = xlrd.open_workbook(u'./data/exam/19-9-26不合格.xls')
    # wb = xlrd.open_workbook(u'./data/excel/2019-08-01省联网联控数据.xls')
    try:
        sht = wb.sheet_by_index(0)
    except xlrd.biffh.XLRDError:
        print "error"
        return
    n = sht.nrows
    veh_set = set()
    rs_dict = {}
    for row in range(n):
        veh = sht.cell_value(row, 0).encode('utf-8')
        reason = sht.cell_value(row, 8)
        # if reason.find(u"缺失") != -1:
        if True:
            # print veh, reason
            veh_set.add(veh)
            rs_dict[veh] = reason
    return veh_set, rs_dict


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
    for i in range(n):
        veh = sht.cell_value(i, 0)
        set_15.add(veh.encode('utf-8'))
    return set_15


def get_add_no():
    conn = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cur = conn.cursor()
    sql = "select distinct (vehicle_num) from tb_gps_simulate where speed_time >= :1 and speed_time < :2" \
          " and sup_type = '1'"
    bt = datetime(2019, 9, 25)
    et = datetime(2019, 9, 26)
    tup = (bt, et)
    cur.execute(sql, tup)
    veh_set = set()
    for item in cur:
        veh = item[0]
        veh_set.add(veh)
    cur.close()
    conn.close()
    return veh_set


def main():
    yst_missing, reasons = fetch_excel()
    for veh in yst_missing:
        print veh, reasons[veh]
    print "yesterday not ok", len(yst_missing)
    all_set = get_15()
    print len(all_set)
    sup_set = get_add_no()
    set_60 = fetch_60_8()
    union = sup_set & yst_missing
    print len(union)
    for veh in union:
        print veh, reasons[veh]
    pass


def fetch_no():
    wb = xlrd.open_workbook(u'./data/11-15报停取消补60+8数据.xlsx')
    sht = wb.sheet_by_index(0)
    n = sht.nrows
    veh_set = set()
    for i in range(1, n):
        veh = sht.cell_value(i, 0)
        veh_set.add(veh.encode('utf-8'))
    return veh_set


def save608():
    set_608 = fetch_60_8()
    set_no = fetch_no()
    rem = set_608 - set_no
    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet('1')
    for i, veh in enumerate(rem):
        ws.write(i, 0, veh)
    wb.save('./data/test60_8.xls')


save608()
