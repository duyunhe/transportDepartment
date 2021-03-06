# -*- coding: utf-8 -*-
# @Time    : 2019/02/20 9:21
# @Author  :
# @Des     : supplement data by 809
# @File    : sup_60_all.py


import cx_Oracle
from datetime import datetime, timedelta
from struct_808 import VehiData
import random
import os
import xlrd
import copy
from veh import get_veh_city, get_veh_3_test, get_filtered_veh
from time import clock
from collections import defaultdict
from strategy_missing import fetch2_0, fetch2_1
from strategy_nodata import fetch1
from strategy_night import fetch_night_0, fetch_night_1
from strategy_city_foreign import fetch_city
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
import json
import urllib2
from geo import gcj02towgs84, bl2xy, calc_dist, calc_segment_coor, xy2bl, transform
import matplotlib.pyplot as plt
from tools import delete_today_emulation
import threading
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'


def debug_time(func):
    def wrapper(*args, **kwargs):
        bt = clock()
        a = func(*args, **kwargs)
        et = clock()
        print "sup.py", func.__name__, "cost", round(et - bt, 2), "secs"
        return a
    return wrapper


def ins(conn, gps_data, sup_cnt, backward):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'0')"
    tup_list = []

    dt = gps_data.speed_time
    gps_data.speed = 0
    for i in range(sup_cnt):
        if backward:
            dt -= timedelta(seconds=20)
        else:
            dt += timedelta(seconds=20)
        vdata = gps_data
        vdata.speed_time = dt
        tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient,
               vdata.speed_time, datetime.now(), 0, vdata.alarm, vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_0704(conn, gps_list):
    """
    插入所有为0704的数据
    :param conn:
    :param gps_list: 0704
    :return:
    """
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'6')"
    tup_list = []
    for vdata in gps_list:
        tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
               vdata.alarm, vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_fix(conn, gps_list):
    """
    first 80 + 8
    :param conn:
    :param gps_list:
    :return:
    """
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'4')"
    tup_list = []
    for vdata in gps_list:
        tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
               vdata.alarm, vdata.state)
        tup_list.append(tup)

    gps_data = gps_list[-1]
    gps_data.speed = 0
    gps_data.state = '000C0002'
    dt = gps_data.speed_time
    for i in range(8):
        if i == 0:
            dt += timedelta(seconds=20)
        else:
            dt += timedelta(minutes=15)
        vdata = copy.copy(gps_data)
        vdata.speed_time = dt
        tup = (
            vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient,
            vdata.speed_time, datetime.now(), 0, vdata.alarm,
            vdata.state)
        tup_list.append(tup)
    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_empty(conn, gps_data, sup_cnt):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'1')"
    tup_list = []
    nw = datetime.now()
    dt = datetime(nw.year, nw.month, nw.day, 6, 0, 0)
    dt += timedelta(minutes=random.randint(30, 120))
    gps_data.speed = 0
    gps_data.state = '000C0003'
    for i in range(sup_cnt):
        dt += timedelta(seconds=20)
        vdata = gps_data
        vdata.speed_time = dt
        tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
               vdata.alarm, vdata.state)
        tup_list.append(tup)
    gps_data.state = '000C0002'
    for i in range(8):
        if i == 0:
            dt += timedelta(seconds=20)
        else:
            dt += timedelta(minutes=15)
        vdata = gps_data
        vdata.speed_time = dt
        tup = (
            vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient,
            vdata.speed_time, datetime.now(), 0, vdata.alarm,
            vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def linear_insert(first_data, last_data):
    itv = (last_data.speed_time - first_data.speed_time).total_seconds()
    time_list = []
    cur = 0
    while cur < itv:
        if 30 < itv - cur <= 60:  # 平分
            cur += (itv - cur) / 2
        elif itv - cur <= 30:
            break
        else:
            cur += 20
        time_list.append(cur)
    first_speed, last_speed = first_data.speed, last_data.speed
    cnt = len(time_list)
    dlng = (last_data.lng - first_data.lng) / (cnt + 1)
    dlat = (last_data.lat - first_data.lat) / (cnt + 1)
    data_list = []
    for i in range(cnt):
        vdata = copy.copy(first_data)
        vdata.lng = (i + 1) * dlng + first_data.lng
        vdata.lat = (i + 1) * dlat + first_data.lat
        vdata.speed = round(random.uniform(first_speed, last_speed), 1)
        vdata.speed_time = first_data.speed_time + timedelta(seconds=time_list[i])
        data_list.append(vdata)
    return data_list


def show_route(route, pt_list):
    x_list, y_list = zip(*route)
    plt.plot(x_list, y_list, c='b', marker='+')
    x_list, y_list = zip(*pt_list)
    plt.plot(x_list, y_list, marker='o', linestyle='')
    plt.show()


def route_insert(route, cnt, begin_data, itv_list):
    """
    按照道路补足点
    :param route: [[px, py], ...] 02坐标系
    :param cnt: 补点的个数
    :param begin_data 起点数据（该段补充数据前面一个点）
    :param itv_list 应补间隔的列表
    :return: [[lng, lat], ...] 84坐标系
    """
    xy_list = []
    for bl in route:
        x, y = bl2xy(bl[1], bl[0])
        xy_list.append([x, y])
    route_dist = 0
    seg_dist = []
    last_xy = xy_list[0]
    for xy in xy_list[1:]:
        dist = calc_dist(xy, last_xy)
        route_dist += dist
        seg_dist.append(dist)
        last_xy = xy
    adist = route_dist / (cnt + 1)
    aspeed = route_dist / (itv_list[-1] + 20) * 3.6     # 估算一下速度
    gene_list = []
    bl_list = []
    pt_list = []
    cur_seg = 0         # 到第几条线段
    offset = 0          # 当前线段的偏移量
    for i in range(cnt):
        offset += adist
        while offset >= seg_dist[cur_seg]:
            offset -= seg_dist[cur_seg]
            cur_seg += 1
        x, y = calc_segment_coor(xy_list[cur_seg], xy_list[cur_seg + 1], offset / seg_dist[cur_seg])
        x += random.randint(-20, 20)
        y += random.randint(-20, 20)
        b, l = xy2bl(x, y)
        wb, wl = gcj02towgs84(b, l)
        gene_list.append([wl, wb])
        pt_list.append([x, y])
        bl_list.append([b, l])

    data_list = []
    max_speed = max(begin_data.speed, aspeed)
    min_speed = min(begin_data.speed, aspeed) * .5
    for i in range(cnt):
        vdata = copy.copy(begin_data)
        vdata.speed_time = begin_data.speed_time + timedelta(seconds=itv_list[i])
        vdata.lng = round(gene_list[i][0], 6)
        vdata.lat = round(gene_list[i][1], 6)
        vdata.x = round(bl_list[i][1], 6)
        vdata.y = round(bl_list[i][0], 6)
        vdata.speed = round(random.uniform(min_speed, max_speed), 1)
        vdata.orient += random.randint(-10, 10)
        if vdata.orient >= 360:
            vdata.orient -= 360
        data_list.append(vdata)

    # show_route(xy_list, pt_list)
    return data_list


def ins_from_line(conn, begin_data, end_data, sup_list):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'3')"
    dt = begin_data.speed_time
    in_list = []
    for data in sup_list:
        if data.speed_time >= end_data.speed_time:
            break
        if data.speed_time > begin_data.speed_time:
            in_list.append(data)

    in_list.append(end_data)
    tup_list = []
    last_data = begin_data
    for i, data in enumerate(in_list):
        itv = data - last_data
        if itv > 30:
            # print 0, last_data.speed_time, data.speed_time
            emu_list = linear_insert(last_data, data)
            for vdata in emu_list:
                tup = (
                    vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
                    vdata.alarm, vdata.state)
                tup_list.append(tup)
        if i != len(in_list) - 1:
            tup = (data.veh_no, data.lng, data.lat, data.speed, data.orient, data.speed_time, datetime.now(), 0,
                   data.alarm, data.state)
            tup_list.append(tup)
        last_data = data

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_from_route(conn, begin_data, end_data, route):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'4')"
    itv = end_data - begin_data
    time_list = []
    cur = 0
    while cur < itv:
        if 30 < itv - cur <= 60:  # 平分
            cur += (itv - cur) / 2
        elif itv - cur <= 30:
            break
        else:
            cur += 20
        time_list.append(cur)
    sup_cnt = len(time_list)
    gps_list = route_insert(route, sup_cnt, begin_data, time_list)
    tup_list = []
    for data in gps_list:
        tup = (data.veh_no, data.lng, data.lat, data.speed, data.orient, data.speed_time, datetime.now(), 0, data.alarm,
               data.state)
        tup_list.append(tup)
    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_from_route2(conn, begin_data, end_data, route):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate2(vehicle_num,longi,lati,px,py,speed," \
              "direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,:6,:7,0,1,:8,:9," \
              "'0',:10,:11,:12,'4')"
    itv = end_data - begin_data
    time_list = []
    cur = 0
    while cur < itv:
        if 30 < itv - cur <= 60:  # 平分
            cur += (itv - cur) / 2
        elif itv - cur <= 30:
            break
        else:
            cur += 20
        time_list.append(cur)
    sup_cnt = len(time_list)
    gps_list = route_insert(route, sup_cnt, begin_data, time_list)
    tup_list = []
    for data in gps_list:
        tup = (data.veh_no, data.lng, data.lat, data.x, data.y, data.speed, data.orient,
               data.speed_time, datetime.now(), 0, data.alarm,
               data.state)
        tup_list.append(tup)
    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def fetch0(conn, veh, org_list):
    if len(org_list) == 0:
        return
    data_list = []
    for vdata in org_list:
        cs = vdata.carstate
        ch = vdata.state[-1]
        if ch == '3' and cs == '1':
            data_list.append(vdata)

    if 0 < len(data_list) < 60:
        sup_cnt = random.randint(61, 100) - len(data_list)
        # print veh, sup_cnt
        if data_list[0].speed_time.hour == 0:
            ins(conn, data_list[-1], sup_cnt, False)
        else:
            ins(conn, data_list[0], sup_cnt, True)


def fetch_with_0704(veh, org_list):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    data_list = []
    for vdata in org_list:
        cs = vdata.carstate
        ch = vdata.state[-1]
        if ch == '3' and (cs == '1' or cs == '3'):
            data_list.append(vdata)

    if 0 < len(data_list) < 60:
        sup_cnt = random.randint(61, 100) - len(data_list)
        # print veh, sup_cnt
        if data_list[0].speed_time.hour == 0:
            ins(db, data_list[-1], sup_cnt, False)
        else:
            ins(db, data_list[0], sup_cnt, True)

    db.close()


def fetch_0704(veh, org_list):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    data_list = []
    last_data = None
    for vdata in org_list:
        cs = vdata.carstate
        if cs == '3':
            if last_data is not None:
                itv = vdata - last_data
                if itv > 0:
                    data_list.append(vdata)
            last_data = vdata
    ins_0704(db, data_list)
    db.close()


def fetch3(veh):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    nw = datetime.now()
    y = nw.year % 100
    m = nw.month
    sql = "select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and speed_time < :2" \
          " order by speed_time".format(y, m, veh)
    td = datetime(nw.year, nw.month, nw.day, 0, 0, 0)
    # nw = td + timedelta(hours=19)
    cursor.execute(sql, (td, nw))
    data_list = []
    sup_list = []
    for item in cursor:
        longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        if (cs == '1' or cs == '0') and (ch == '3' or ch == '2' or ch == '0'):
            data_list.append(vdata)
        if cs == '3':
            sup_list.append(vdata)

    last_data = None
    for data in data_list:
        if last_data is not None:
            itv = (data.speed_time - last_data.speed_time).total_seconds()
            if last_data.state[-1] == '3' and data.state[-1] == '3':
                if itv > 30:
                    print last_data.speed_time, data.speed_time, itv
                    ins_from_line(db, last_data, data, sup_list)
        last_data = data
    cursor.close()
    db.close()


def polyline2xy(polyline):
    xy_list = []
    items = polyline.split(';')
    for item in items:
        x, y = map(float, item.split(','))
        xy_list.append([x, y])
    return xy_list


def get_route(px0, py0, px1, py1):
    req = "https://restapi.amap.com/v3/direction/driving?origin={0},{1}&destination={2},{3}" \
          "&extensions=base&output=json&key=0a54a59bdc431189d9405b3f2937921a".format(px0, py0, px1, py1)
    route_list = []
    try:
        f = urllib2.urlopen(req)
        response = f.read()
        temp = json.loads(response)
        if temp["status"] == u"1":
            route = temp["route"]
            path = route["paths"][0]
            steps = path['steps']
            for i, step in enumerate(steps):
                p = step['polyline']
                xy_list = polyline2xy(p)
                if i == 0:
                    route_list.extend(xy_list)
                else:
                    route_list.extend(xy_list[1:])
    except Exception as e:
        print "error", e.message, px0, py0, px1, py1
    return route_list


def fetch4(veh):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    nw = datetime.now()
    y = nw.year % 100
    m = nw.month
    sql = "select longi, lati, px, py, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and speed_time < :2" \
          " order by speed_time".format(y, m, veh)
    td = datetime(nw.year, nw.month, nw.day, 0, 0, 0)
    # nw = td + timedelta(hours=19)
    cursor.execute(sql, (td, nw))

    data_list = []
    for item in cursor:
        longi, lati, px, py, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, px, py, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        if (cs == '1' or cs == '0') and (ch == '3' or ch == '2' or ch == '0'):
            data_list.append(vdata)

    last_data = None
    for data in data_list:
        if last_data is not None:
            itv = data - last_data
            if last_data.state[-1] == '3' and data.state[-1] == '3':
                if itv > 30:
                    route = get_route(last_data.x, last_data.y, data.x, data.y)
                    print last_data.speed_time, data.speed_time, itv
                    # ins_from_route2(db, last_data, data, route)
        last_data = data
    cursor.close()
    db.close()


def fetch_fix(veh):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    nw = datetime.now()
    y = nw.year % 100
    m = nw.month
    sql = "select longi, lati, px, py, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and speed_time < :2" \
          " order by speed_time".format(y, m, veh)
    td = datetime(nw.year, nw.month, nw.day, 0, 0, 0)
    # nw = td + timedelta(hours=19)
    cursor.execute(sql, (td, nw))
    data_list = []

    for item in cursor:
        longi, lati, px, py, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, px, py, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        if cs == '1' and ch == '3':
            data_list.append(vdata)
    if len(data_list) > 0:
        valid_cnt = random.randint(81, 100)
        ins_fix(db, data_list[:valid_cnt])

    cursor.close()
    db.close()


def get_veh0():
    veh_list = []
    xl = xlrd.open_workbook('sup60.xlsx')
    sheet = xl.sheet_by_index(0)
    n = sheet.nrows
    for i in range(n):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
    print len(veh_list)
    return veh_list


def get_veh1():
    veh_list = []
    xl = xlrd.open_workbook('sup60_8.xlsx')
    sheet = xl.sheet_by_index(0)
    n = sheet.nrows
    for i in range(n):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
    print "no data", len(veh_list)
    return veh_list


def get_veh2_without_accoff_filter():
    veh_list = []
    xl = xlrd.open_workbook('sup15.xlsx')
    sheet = xl.sheet_by_index(0)
    n = sheet.nrows
    for i in range(n):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        if str_val[-1] == ' ':
            str_val = str_val[:-1]
        veh_list.append(str_val)
    print "all no filter", len(veh_list), "veh"
    return veh_list


def get_veh2():
    veh_list = ['浙AL0U38']
    return veh_list


def get_veh2_with_accoff_filter():
    veh_list = []
    xl = xlrd.open_workbook('sup15.xlsx')
    sheet = xl.sheet_by_index(1)
    n = sheet.nrows
    for i in range(n):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        if str_val[-1] == ' ':
            str_val = str_val[:-1]
        veh_list.append(str_val)
    print "all filter", len(veh_list), "veh"
    return veh_list


def get_veh3():
    veh_list = []
    xl = xlrd.open_workbook('sup_ca.xlsx')
    sheet = xl.sheet_by_index(0)
    for i in range(4):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
    return veh_list


def get_veh4():
    veh_list = ['浙A1B825', '浙A1H600', '浙A2B377']
    return veh_list


def get_veh_0704():
    """
    0704补传时需要检查整天的记录，这些车辆单独分析
    :return:
    """
    veh_list = ['浙A9H189', '浙A9S685', '浙A8K587', '浙A1B826']
    return veh_list


@debug_time
def sup60(data_dict):
    """
    查总量不合格，补60条  tb_gps_simulate表中填sup_type = 0
    :param data_dict 总表
    :return:
    """
    veh_list = get_veh0()
    fix_list = get_veh4()       # 三辆车用其他方式补数据
    veh_0704_list = get_veh_0704()
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    for i, veh in enumerate(veh_list):
        # print veh
        if veh in veh_0704_list:
            fetch_with_0704(veh, data_dict[veh])
            continue
        if veh in fix_list:
            continue
        fetch0(db, veh, data_dict[veh])
    print "sup60 over"
    db.close()


def sup_0704(data_dict):
    veh_0704_list = get_veh_0704()
    for veh in veh_0704_list:
        fetch_0704(veh, data_dict[veh])


@debug_time
def sup_missing_test():
    """
    补缺失数据 sup_type = 2
    :return:
    """
    bt = datetime(2019, 8, 18)
    et = bt + timedelta(days=1)
    veh_list = get_veh2()
    for veh in veh_list:
        # print veh
        fetch2_0(veh, bt, et)
    return


def thread_fetch_2_0(veh_list):
    now = datetime.now()
    bt = datetime(now.year, now.month, now.day, 0)
    for veh in veh_list:
        fetch2_0(veh, bt, now)


def thread_fetch_2_1(veh_list):
    now = datetime.now()
    bt = datetime(now.year, now.month, now.day, 0)
    for veh in veh_list:
        fetch2_1(veh, bt, now)


def thread_fetch_city(veh_list, filter_set):
    now = datetime.now()
    for veh in veh_list:
        fetch_city(veh, now, filter_set)


@debug_time
def sup_missing():
    """
    补缺失数据 sup_type = 2
    :return:
    """
    trds = []
    veh_list = get_veh2_without_accoff_filter()
    num = min(len(veh_list), 30)
    for i in range(num):
        t = threading.Thread(target=thread_fetch_2_0, args=(veh_list[i::num], ))
        trds.append(t)
    for t in trds:
        t.start()
    for t in trds:
        t.join()

    trds = []
    veh_list = get_veh2_with_accoff_filter()        # acc 关过滤
    num = min(len(veh_list), 30)
    for i in range(num):
        t = threading.Thread(target=thread_fetch_2_1, args=(veh_list[i::num],))
        trds.append(t)
    for t in trds:
        t.start()
    for t in trds:
        t.join()

    print "sup 8&15 over"


def thread_fetch1(veh_list):
    now = datetime.now()
    bt = datetime(now.year, now.month, now.day)
    yst = bt - timedelta(days=7)
    for veh in veh_list:
        fetch1(veh, bt, yst, now)


@debug_time
def sup_no_data():
    """
    补无数据 sup_type = 1
    :return:
    """
    veh_list = get_veh1()

    trds = []
    num = min(30, len(veh_list))       # 30个线程一起查，每个线程查其中一批车辆
    for i in range(num):
        t = threading.Thread(target=thread_fetch1, args=(veh_list[i::num], ))
        trds.append(t)
    for t in trds:
        t.start()
    for t in trds:
        t.join()

    print "sup 60+8 over"


def sup_line():
    """
    补淳安千岛湖 sup_type = 3
    :return:
    """
    veh_list = get_veh3()
    for veh in veh_list:
        print veh
        fetch3(veh)
    print "sup chunan over"


def sup_line_amap():
    """
    省道、高速等 sup_type = 4
    :return:
    """
    veh_list = get_veh3()
    for veh in veh_list:
        print veh
        fetch4(veh)
    print "sup chunan over"


def sup_fix():
    """
    固定补[80, 100)+8 sup_type=4
    :return:
    """
    veh_list = get_veh4()
    for veh in veh_list:
        fetch_fix(veh)


@debug_time
def get_data(data_dict, begin_time, end_time):
    """
    获取一段时间内的所有非出租数据库定位数据
    并按照车牌号码组织成字典
    :param data_dict: 数据字典 key是车牌号码，value是一个VehiData list
    :param begin_time: 开始时间 datetime
    :param end_time: 结束时间 datetime 两个时间在同一天
    :return:
    """
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    y = begin_time.year % 100
    m = begin_time.month
    sql = "select vehicle_num, longi, lati, px, py, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where speed_time >= :1 and speed_time < :2" \
          " order by speed_time".format(y, m)
    cursor.execute(sql, (begin_time, end_time))
    for item in cursor:
        veh, longi, lati, px, py, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        if cs == '1' or cs == '3':      # 只要精确数据
            vdata = VehiData(longi, lati, px, py, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
            data_dict[veh].append(vdata)

    cursor.close()
    db.close()


@debug_time
def sup_data():
    # delete_all_data()
    td_dict = defaultdict(list)
    # yst_dict = defaultdict(list)
    now = datetime.now()
    td = datetime(now.year, now.month, now.day)
    # yst = td - timedelta(days=1)
    get_data(td_dict, td, now)
    # get_data(yst_dict, yst, td)
    sup60(td_dict)
    sup_missing()
    sup_no_data()
    sup_city()
    print "sup data completed", datetime.now()


def sup_city():
    veh_list = get_veh_city()
    off_filter_set = get_filtered_veh()
    trds = []
    num = min(20, len(veh_list))  # 20个线程一起查，每个线程查其中一批车辆
    for i in range(num):
        t = threading.Thread(target=thread_fetch_city, args=(veh_list[i::num], off_filter_set))
        trds.append(t)
    for t in trds:
        t.start()
    for t in trds:
        t.join()

    print "sup city over"


def sup_data_0704():
    now = datetime.now()
    td = datetime(now.year, now.month, now.day, hour=19)
    td_dict = defaultdict(list)
    get_data(td_dict, td, now)
    sup_0704(td_dict)
    print "night 0704 completed", datetime.now()


def sup_night_test():
    now = datetime.now()
    yst = now - timedelta(days=1)
    yst = datetime(yst.year, yst.month, yst.day, 23, 45)
    veh_list = get_veh2_without_accoff_filter()
    for veh in veh_list:
        fetch_night_0(veh, yst)
    print "test over"


def sup_night():
    now = datetime.now()

    veh_list = get_veh2_without_accoff_filter()
    for veh in veh_list:
        fetch_night_0(veh, now)

    veh_list = get_veh2_with_accoff_filter()        # acc 关过滤
    for veh in veh_list:
        fetch_night_1(veh, now)
    print "night completed", datetime.now()


delete_today_emulation()
# sup_data()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    scheduler.add_job(sup_data, 'cron', hour='19', minute='00', max_instances=10)
    scheduler.add_job(sup_night, 'cron', hour='23', minute='30', max_instances=10)
    scheduler.start()
