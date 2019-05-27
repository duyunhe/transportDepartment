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
from time import clock
from collections import defaultdict
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
import json
import urllib2
from geo import gcj02towgs84, bl2xy, calc_dist, calc_segment_coor, xy2bl, transform
import matplotlib.pyplot as plt
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


def ins_15(conn, first_data, last_data):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'2')"
    tup_list = []
    emu_list = linear_insert(first_data, last_data)

    for vdata in emu_list:
        tup = (
        vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0, vdata.alarm,
        vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_8(conn, first_data, last_data):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'2')"
    dt = first_data.speed_time
    first_data.speed = 0
    first_data.state = "000C0002"
    tup_list = []
    for i in range(8):
        if i == 0:
            dt += timedelta(seconds=20)
        else:
            dt += timedelta(minutes=15)
        if dt < last_data.speed_time:
            vdata = first_data
            vdata.speed_time = dt
            tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
                   vdata.alarm, vdata.state)
            tup_list.append(tup)
        else:
            break

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_from_0704(conn, begin_data, end_data, sup_list):
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
            print 0, last_data.speed_time, data.speed_time
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
        print veh, sup_cnt
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
    for vdata in org_list:
        cs = vdata.carstate
        if cs == '3':
            data_list.append(vdata)
    ins_0704(db, data_list)
    db.close()


def fetch1(veh, td_list, yst_list):
    """
    :param veh: 车牌号码
    :param td_list: 今天的车辆信息列表
    :param yst_list: 昨天的车辆信息列表
    :return: 
    """
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")

    data_list = []
    last_valid_data = None
    for vdata in td_list:
        mdtstatus, cs = vdata.state, vdata.carstate
        ch = mdtstatus[-1]
        if ch == '3' and cs == '1':
            data_list.append(vdata)
        if ch == '3' or ch == '2':
            last_valid_data = vdata
    # 无数据时还需要去查前一天
    if len(data_list) == 0:
        for vdata in yst_list:
            mdtstatus, cs = vdata.state, vdata.carstate
            ch = mdtstatus[-1]
            if ch == '3' or ch == '2':
                last_valid_data = vdata
        if last_valid_data is not None:
            print veh, "empty"
            ins_empty(
                conn=db, gps_data=last_valid_data, sup_cnt=random.randint(60, 70)
            )
    db.close()


def fetch2(veh):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    nw = datetime.now()
    y = nw.year % 100
    m = nw.month
    # 查今天非0704数据
    sql = "select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and speed_time < :2" \
          " order by speed_time".format(y, m, veh)
    td = datetime(nw.year, nw.month, nw.day, 0, 0, 0)
    cursor.execute(sql, (td, nw))
    data_list = []
    for item in cursor:
        longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        if (cs == '1' or cs == '0') and (ch == '3' or ch == '2' or ch == '0'):
            data_list.append(vdata)

    last_data = None
    for data in data_list:
        if last_data is not None:
            itv = (data.speed_time - last_data.speed_time).total_seconds()
            if last_data.state[-1] == '3' and data.state[-1] == '3':
                if 30 < itv < 900:
                    # print "0003", veh, last_data.speed_time, data.speed_time, itv
                    ins_15(db, last_data, data)
                elif itv > 900:
                    # print "0002", veh, last_data.speed_time, data.speed_time, itv
                    ins_8(db, last_data, data)

        last_data = data
    cursor.close()
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
                    ins_from_0704(db, last_data, data, sup_list)
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
    for i in range(2738):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
    print len(veh_list)
    return veh_list


def get_veh1():
    veh_list = []
    xl = xlrd.open_workbook('sup60_8.xlsx')
    sheet = xl.sheet_by_index(0)
    for i in range(2677):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
    print len(veh_list)
    return veh_list


def get_veh2():
    veh_list = []
    xl = xlrd.open_workbook('sup15.xlsx')
    sheet = xl.sheet_by_index(0)
    for i in range(100):
        val = sheet.cell(i, 0).value
        str_val = val.encode('utf-8')
        veh_list.append(str_val)
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
    veh_list = ['浙A6G680']
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
def sup60and8(td_dict, yst_dict):
    """
    完全无数据，补60+8   sup_type = 1
    :return:
    """
    veh_list = get_veh1()
    for i, veh in enumerate(veh_list):
        # print veh
        fetch1(veh, td_dict[veh], yst_dict[veh])
        if i % 100 == 0:
            print "sup60&8", i
    print "sup60and8 over"


@debug_time
def sup15():
    """
    补缺失数据 sup_type = 2
    :return:
    """
    veh_list = get_veh2()
    for veh in veh_list:
        # print veh
        fetch2(veh)
    print "sup15 over"


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
          " from tb_gps_{0}{1:02} where speed_time >= :1 and speed_time < :2 and vehicle_num = '浙A6G680'" \
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
    yst_dict = defaultdict(list)
    now = datetime.now()
    td = datetime(now.year, now.month, now.day)
    yst = td - timedelta(days=1)
    get_data(td_dict, td, now)
    # get_data(yst_dict, yst, td)
    sup60(td_dict)
    sup_0704(td_dict)
    # sup60and8(td_dict, yst_dict)
    sup15()
    sup_fix()
    # sup_line()
    # static_data()
    print "全部完成", datetime.now()


def static_data():
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    sql = "select count(*) from tb_gps_simulate"
    now = datetime.now()
    dt = datetime(now.year, now.month, now.day)
    cnt = 0
    cursor.execute(sql)
    for item in cursor:
        cnt = int(item[0])
    ins_sql = "insert into tb_gps_simulate3 values(:1,:2)"
    cursor.execute(ins_sql, (cnt, dt))
    db.commit()
    cursor.close()
    db.close()


def delete_all_data():
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    sql = "delete from tb_gps_simulate"
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()


sup_data()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    scheduler.add_job(sup_data, 'cron', hour='19', minute='00', max_instances=10)
    scheduler.start()
