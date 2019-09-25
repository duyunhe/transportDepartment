# -*- coding: utf-8 -*-
# @Time    : 2019/8/22 14:47
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : strategy_night.py

from struct_808 import VehiData
import cx_Oracle
from datetime import datetime, timedelta
from tools import is_acc_on
from strategy_missing import ins_15, ins_8, ins_off


def get_emu_data(conn, bt, et, veh):
    cur = conn.cursor()
    sql = "select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_simulate where vehicle_num = '{0}' and speed_time >= :1 " \
          "and speed_time < :2 order by speed_time".format(veh)
    tup = (bt, et)
    cur.execute(sql, tup)
    data_list = []
    for item in cur:
        longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        data = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        data_list.append(data)
    cur.close()
    return data_list


def get_gps_data(conn, bt, et, veh, filtered=False):
    cursor = conn.cursor()
    y = bt.year % 100
    m = bt.month
    # 查今天无过滤数据
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
        if filtered:
            if ch == '3':
                data_list.append(vdata)
        else:
            if ch == '3' or ch == '2':
                data_list.append(vdata)
    cursor.close()
    return data_list


def fetch_night_0(veh, now):
    """
    补缺失
    :param veh:
    :param now:
    :return:
    """
    bt = datetime(now.year, now.month, now.day, 17)
    et = now
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")

    data_list = []
    gps_data = get_gps_data(db, bt, et, veh)
    emu_data = get_emu_data(db, bt, et, veh)
    data_list.extend(gps_data)
    data_list.extend(emu_data)
    data_list.sort()

    sup_type = "2.0"
    last_data = None
    for data in data_list:
        if last_data is not None:
            itv = (data.speed_time - last_data.speed_time).total_seconds()
            if is_acc_on(data) and is_acc_on(last_data):
                if 30 < itv <= 900:
                    # print "0003 ", veh, last_data.speed_time, data.speed_time, itv
                    ins_15(db, last_data, data, sup_type)
                elif itv > 900:
                    # print "0002", veh, last_data.speed_time, data.speed_time, itv
                    ins_8(db, last_data, data.speed_time, sup_type)
            else:  # once acc off
                if itv > 1000:  # maybe 1800 seconds or more
                    ins_off(db, last_data, data, sup_type)
        last_data = data

    if last_data:
        itv = (et - last_data.speed_time).total_seconds()
        if itv >= 900:
            ins_8(db, last_data, et, sup_type)

    db.close()


def fetch_night_1(veh, now):
    bt = datetime(now.year, now.month, now.day, 17)
    et = now
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")

    data_list = []
    gps_data = get_gps_data(db, bt, et, veh)
    emu_data = get_emu_data(db, bt, et, veh)
    data_list.extend(gps_data)
    data_list.extend(emu_data)
    data_list.sort()

    sup_type = "2.1"
    last_data = None
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

    if last_data:
        itv = (et - last_data.speed_time).total_seconds()
        if itv >= 900:
            ins_8(db, last_data, et, sup_type)

    db.close()
