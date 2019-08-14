# -*- coding: utf-8 -*-
# @Time    : 2019/08/12 9:21
# @Author  :
# @Des     : supplement data strategy
# @File    : strategy_missing.py


import cx_Oracle
from datetime import datetime, timedelta
import copy
import random
from struct_808 import VehiData
from tools import is_acc_off, is_acc_on


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


def ins_off(conn, first_data, last_data, sup_type):
    """
    :param conn:
    :param first_data: 第一个数据
    :param last_data: 最后一个数据
    :return:
    """
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,:11)"
    dt = first_data.speed_time
    first_data.speed = 0
    first_data.state = "000C0002"
    tup_list = []
    for i in range(96):
        dt += timedelta(minutes=15)
        if dt < last_data.speed_time:
            vdata = first_data
            vdata.speed_time = dt
            tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
                   vdata.alarm, vdata.state, sup_type)
            tup_list.append(tup)
        else:
            break

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_8(conn, first_data, last_time, sup_type):
    """
    过滤后，只需要补8条
    :param conn:
    :param first_data: 第一个数据
    :param last_time: 最后一个数据的时间
    :param sup_type: 补类型
    :return:
    """
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,:11)"
    dt = first_data.speed_time
    first_data.speed = 0
    first_data.state = "000C0002"
    tup_list = []
    for i in range(8):
        if i == 0:
            dt += timedelta(seconds=25)
        else:
            dt += timedelta(minutes=15)
        if dt < last_time:
            vdata = first_data
            vdata.speed_time = dt
            tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0,
                   vdata.alarm, vdata.state, sup_type)
            tup_list.append(tup)
        else:
            break

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_15(conn, first_data, last_data, sup_type):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,:11)"
    tup_list = []
    emu_list = linear_insert(first_data, last_data)

    for vdata in emu_list:
        tup = (vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient,
               vdata.speed_time, datetime.now(), 0, vdata.alarm, vdata.state, sup_type)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def fetch2_0(veh, bt, et):
    """
    近原地补传，此处网关未过滤ACC关，139辆
    见代码
    :param veh:
    :param bt:
    :param et:
    :return:
    """
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    nw = datetime.now()
    y = nw.year % 100
    m = nw.month
    # 查今天非0704数据
    sql = "select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and carstate = '1' " \
          "and speed_time < :2 order by speed_time".format(y, m, veh)
    cursor.execute(sql, (bt, et))
    data_list = []
    for item in cursor:
        longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        # cs 为2 3 是补报
        # ch 为1 是非精确
        if ch == '3' or ch == '2' or ch == '0':
            data_list.append(vdata)

    sup_type = "2.0"
    last_data = None
    for data in data_list:
        if last_data is not None:
            itv = (data.speed_time - last_data.speed_time).total_seconds()
            if is_acc_on(data) and is_acc_on(last_data):
                if 30 < itv < 900:
                    # print "0003 ", veh, last_data.speed_time, data.speed_time, itv
                    ins_15(db, last_data, data, sup_type)
                elif itv >= 900:
                    # print "0002", veh, last_data.speed_time, data.speed_time, itv
                    ins_8(db, last_data, data.speed_time, sup_type)
            else:       # once acc off
                if itv > 1000:      # maybe 1800 seconds or more
                    ins_off(db, last_data, data, sup_type)
        last_data = data

    if last_data and is_acc_on(last_data):
        itv = (et - last_data.speed_time).total_seconds()
        if itv > 900:
            ins_8(db, last_data, et, sup_type)

    cursor.close()
    db.close()


def fetch2_1(veh, bt, et):
    """
    近原地补传，此处网关过滤ACC关，51辆车
    15分钟内的采取直线补
    15分钟以上的使用ACC关补
    最后一条ACC开一直到end time时间，一直补ACC关
    :param veh:
    :param bt:
    :param et: end time
    :return:
    """
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    nw = datetime.now()
    y = nw.year % 100
    m = nw.month
    # 查今天非0704精确数据
    sql = "select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and carstate = '1' and speed_time >= :1" \
          " and speed_time < :2" \
          " order by speed_time".format(y, m, veh)
    cursor.execute(sql, (bt, et))
    data_list = []
    for item in cursor:
        longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        # cs 为2 3 是补报
        # ch 为3 是精确ACC开数据
        # 此处，只留精确ACC开，因其他数据都被过滤
        if ch == '3':
            data_list.append(vdata)

    last_data = None
    sup_type = "2.1"
    for data in data_list:
        if last_data is not None:
            itv = (data.speed_time - last_data.speed_time).total_seconds()
            if 30 < itv < 900:
                # print "0003", veh, last_data.speed_time, data.speed_time, itv
                ins_15(db, last_data, data, sup_type)
            elif itv >= 900:
                # print "0002", veh, last_data.speed_time, data.speed_time, itv
                ins_8(db, last_data, data.speed_time, sup_type)
        last_data = data

    # 最后还要补8条
    if last_data:
        itv = (et - last_data.speed_time).total_seconds()
        if itv > 900:
            ins_8(db, last_data, et, sup_type)

    cursor.close()
    db.close()


