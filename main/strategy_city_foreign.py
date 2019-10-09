# -*- coding: utf-8 -*-
# @Time    : 2019/9/23 14:47
# @Author  : yhdu@tongwoo.cn
# @简介    : 市外事
# @File    : strategy_city_foreign.py

from strategy_night import get_gps_data, get_emu_data
from datetime import datetime, timedelta
import cx_Oracle
import random
from tools import is_acc_on, is_acc_off


def ins_on(conn, gps_data, sup_cnt, backward):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'3')"
    tup_list = []

    dt = gps_data.speed_time
    gps_data.speed = 0
    gps_data.state = '000C0003'
    lng, lat = gps_data.lng, gps_data.lat
    for i in range(sup_cnt):
        if backward:
            dt -= timedelta(seconds=20)
        else:
            dt += timedelta(seconds=20)
        vdata = gps_data
        vdata.speed_time = dt
        if i % 60 == 0:
            lng += random.uniform(-0.000005, 0.000005)
            lat += random.uniform(-0.000005, 0.000005)
        tup = (vdata.veh_no, lng, lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0, vdata.alarm,
               vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_one(conn, gps_data):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,'3')"
    tup_list = []

    dt = gps_data.speed_time
    gps_data.speed = 0
    gps_data.state = '000C0003'
    lng, lat = gps_data.lng, gps_data.lat
    for i in range(1):
        dt += timedelta(seconds=5)
        vdata = gps_data
        vdata.speed_time = dt
        lng += random.uniform(-0.000005, 0.000005)
        lat += random.uniform(-0.000005, 0.000005)
        tup = (vdata.veh_no, lng, lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0, vdata.alarm,
               vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def ins_empty(conn, gps_data, sup_cnt, sup_type='3'):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,{0})".format(sup_type)
    tup_list = []
    dt = gps_data.speed_time + timedelta(seconds=random.randint(20, 30))
    gps_data.speed = 0
    gps_data.state = '000C0003'

    lng, lat = gps_data.lng, gps_data.lat
    for i in range(sup_cnt):
        vdata = gps_data
        vdata.speed_time = dt
        if i % 60 == 0:
            lng += random.uniform(-0.000005, 0.000005)
            lat += random.uniform(-0.000005, 0.000005)
        tup = (vdata.veh_no, lng, lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0, vdata.alarm,
               vdata.state)
        tup_list.append(tup)
        dt += timedelta(seconds=20)
    gps_data.state = '000C0002'
    for i in range(8):
        if i == 0:
            dt += timedelta(seconds=20)
        else:
            dt += timedelta(minutes=15)
        vdata = gps_data
        vdata.speed_time = dt
        tup = (
        vdata.veh_no, vdata.lng, vdata.lat, vdata.speed, vdata.orient, vdata.speed_time, datetime.now(), 0, vdata.alarm,
        vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def fetch_city(veh, now, filter_set):
    bt = datetime(now.year, now.month, now.day)
    et = now
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    filtered = veh in filter_set
    gps_list = get_gps_data(db, bt, et, veh)
    emu_list = get_emu_data(db, bt, et, veh)
    data_list = gps_list[::]
    data_list.extend(emu_list)
    data_list.sort()

    sup_type = "3"
    on_cnt, off_cnt = 0, 0
    last_data = None

    def_lng, def_lat = None, None
    all_same = True
    for data in data_list:
        if is_acc_on(data):
            on_cnt += 1
            if def_lng is None:
                def_lng, def_lat = data.lng, data.lat
            else:
                if def_lng != data.lng or def_lat != data.lat:
                    all_same = False
        elif not filtered:
            off_cnt += 1

    bt += timedelta(minutes=4)
    sup_cnt = random.randint(60, 70)
    if on_cnt == 0 and 0 < off_cnt < 60:
        print veh, "acc off not enough"
        itv0 = (data_list[0].speed_time - bt).total_seconds()
        itv1 = (et - data_list[-1].speed_time).total_seconds()
        if itv0 > 3600 * 3:
            last_valid_data = data_list[0]
            last_valid_data.speed_time = bt
            ins_empty(conn=db, gps_data=last_valid_data, sup_cnt=sup_cnt, sup_type=sup_type)
        elif itv1 > 3600 * 3:
            last_valid_data = data_list[-1]
            ins_empty(conn=db, gps_data=last_valid_data, sup_cnt=sup_cnt, sup_type=sup_type)
        else:
            for data in data_list:
                if last_data is not None:
                    itv = data - last_data
                    if itv > 3600 * 3:
                        ins_empty(conn=db, gps_data=last_data, sup_cnt=sup_cnt, sup_type=sup_type)
                last_data = data

    if all_same:
        print veh, "acc on, all same px & py"
        on_cnt = 0
        for data in data_list:
            if is_acc_on(data):
                on_cnt += 1
                if on_cnt % 80 == 79:
                    ins_one(conn=db, gps_data=data)

    db.close()
