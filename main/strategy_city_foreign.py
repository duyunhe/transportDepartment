# -*- coding: utf-8 -*-
# @Time    : 2019/9/23 14:47
# @Author  : yhdu@tongwoo.cn
# @简介    : 市外事
# @File    : strategy_city_foreign.py

from strategy_night import get_gps_data
from datetime import datetime, timedelta
import cx_Oracle
from strategy_nodata import ins_empty
from tools import is_acc_on


def fetch_city(veh, now):
    bt = datetime(now.year, now.month, now.day)
    et = now
    db = cx_Oracle.connect("lklh", "lklh", "192.168.0.113/orcl")

    data_list = get_gps_data(db, bt, et, veh)
    data_list.sort()

    sup_type = "3"
    on_cnt, off_cnt = 0, 0
    last_data = None
    for data in data_list:
        if is_acc_on(data):
            on_cnt += 1
        else:
            off_cnt += 1

    bt += timedelta(minutes=4)
    if on_cnt == 0 and 0 < off_cnt < 60:
        itv0 = (data_list[0].speed_time - bt).total_seconds()
        itv1 = (et - data_list[-1].speed_time).total_seconds()
        if itv0 > 3600 * 3:
            last_valid_data = data_list[0]
            last_valid_data.speed_time = bt
            ins_empty(conn=db, gps_data=last_valid_data, sup_cnt=62, sup_type=sup_type)
        elif itv1 > 3600 * 3:
            last_valid_data = data_list[-1]
            ins_empty(conn=db, gps_data=last_valid_data, sup_cnt=62, sup_type=sup_type)
        else:
            for data in data_list:
                if last_data is not None:
                    itv = data - last_data
                    if itv > 3600 * 3:
                        ins_empty(conn=db, gps_data=last_data, sup_cnt=62, sup_type='3.1')
                last_data = data

    db.close()
