# -*- coding: utf-8 -*-
# @Time    : 2019/8/21 13:55
# @Author  : yhdu@tongwoo.cn
# @简介     : type 1
# @File    : strategy_nodata.py


import xlrd
import cx_Oracle
from datetime import datetime, timedelta
import copy
import random
from struct_808 import VehiData


def get_veh1_():
    return ['浙A09527D', '浙A1B150', '浙A8D273', '浙A8D253']


def ins_empty(conn, gps_data, sup_cnt, sup_type='1'):
    cursor = conn.cursor()
    ins_sql = "insert into tb_gps_simulate(vehicle_num,longi,lati,speed,direction,state,carstate,speed_time,db_time," \
              "sign,altitude,alarmstatus,mdtstatus,sup_type) values(:1,:2,:3,:4,:5,0,1,:6,:7," \
              "'0',:8,:9,:10,{0})".format(sup_type)
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
        lng, lat = vdata.lng + random.uniform(-0.00001, 0.00001), vdata.lat + random.uniform(-0.00001, 0.00001)
        lng, lat = round(lng, 6), round(lat, 6)
        tup = (
            vdata.veh_no, lng, lat, vdata.speed, vdata.orient,
            vdata.speed_time, datetime.now(), 0, vdata.alarm,
            vdata.state)
        tup_list.append(tup)

    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()


def fetch1(veh, bt, yst, nw):
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")
    cursor = db.cursor()
    y = nw.year % 100
    m = nw.month
    # 查今天数据
    sql = "select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
          " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and carstate = '1' " \
          "and speed_time < :2 order by speed_time".format(y, m, veh)
    cursor.execute(sql, (bt, nw))
    data_list = []
    acc_on, acc_off = 0, 0
    for item in cursor:
        longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, cs = item[:]
        vdata = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh, None, alarmstatus, mdtstatus, direction, cs)
        ch = mdtstatus[-1]
        # cs 为2 3 是补报
        # ch 为1 是非精确
        if ch == '3' or ch == '2':
            data_list.append(vdata)
        if ch == '3':
            acc_on += 1
        if ch == '2':
            acc_off += 1
    # print veh, acc_on, acc_off
    if len(data_list) != 0:
        cursor.close()
        db.close()
        return
    # 查昨日数据
    y, m = yst.year % 100, yst.month
    sql = "select longi, lati, speed, angle, stime, alarmstatus, carstate from tb_mdt_status t" \
          " where vehi_num = '{0}' and carstate = '1'".format(veh)
    cursor.execute(sql)
    # sql = "select * from (select longi, lati, speed, direction, speed_time, mdtstatus, alarmstatus, carstate" \
    #       " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and " \
    #       "speed_time < :2 and carstate = '1' order by speed_time desc) where rownum = 1".format(y, m, veh)
    # cursor.execute(sql, (yst, bt))
    last_valid_data = None
    for item in cursor:
        longi, lati, speed, direction, speed_time, alarmstatus, cs = item[:]
        last_valid_data = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh,
                                   None, alarmstatus, "000C0003", direction, cs)
    if last_valid_data is None:
        # print veh, "yesterday mdt no data"
        sql = "select longi, lati, speed, direction, speed_time, alarmstatus, carstate" \
              " from tb_gps_{0}{1:02} where vehicle_num = '{2}' and speed_time >= :1 and " \
              " speed_time < :2 and carstate = '1' order by speed_time desc".format(y, m, veh)
        cursor.execute(sql, (bt - timedelta(days=2), bt))
        for item in cursor:
            longi, lati, speed, direction, speed_time, alarmstatus, cs = item[:]
            last_valid_data = VehiData(longi, lati, 0, 0, 0, speed, speed_time, veh,
                                       None, alarmstatus, "000C0003", direction, cs)
            break
        if last_valid_data is None:
            print veh, "yesterday history really no data"
            cursor.close()
            db.close()
            return
    ins_empty(db, last_valid_data, random.randint(60, 80))
    cursor.close()
    db.close()
