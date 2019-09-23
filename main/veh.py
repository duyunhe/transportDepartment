# -*- coding: utf-8 -*-
# @Time    : 2019/8/21 14:10
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : veh.py


import xlrd
import cx_Oracle
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'


def get_filtered_veh():
    fp = open("./data/filterAccOffISU.txt")
    line = fp.readline()
    isu_list = line.strip('\n').split(',')
    fp.close()

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
    cur.close()
    conn.close()
    return set_400


def get_veh1():
    veh_list = []
    xl = xlrd.open_workbook('./data/sup60_8.xlsx')
    sheet = xl.sheet_by_index(0)
    for i in range(4):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
    print "no data", len(veh_list)
    return veh_list


def get_veh2_without_accoff_filter():
    veh_list = set()
    xl = xlrd.open_workbook('./data/sup15.xlsx')
    sheet = xl.sheet_by_index(0)
    n = sheet.nrows
    for i in range(n):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        if str_val[-1] == ' ':
            str_val = str_val[:-1]
        veh_list.add(str_val)

    no_filter_set = list(veh_list - get_filtered_veh())
    print "all no filter", len(no_filter_set), "veh"
    return no_filter_set


def get_veh2():
    veh_list = ['浙A2X302']
    return veh_list


def get_veh2_with_accoff_filter():
    veh_list = set()
    xl = xlrd.open_workbook('./data/sup15.xlsx')
    sheet = xl.sheet_by_index(0)
    n = sheet.nrows
    for i in range(n):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        if str_val[-1] == ' ':
            str_val = str_val[:-1]
        veh_list.add(str_val)
    no_filter_set = list(veh_list & get_filtered_veh())
    print "all no filter", len(no_filter_set), "veh"
    return no_filter_set


get_veh2_without_accoff_filter()
get_veh2_with_accoff_filter()
