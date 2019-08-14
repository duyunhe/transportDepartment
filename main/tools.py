# -*- coding: utf-8 -*-
# @Time    : 2019/08/13 9:21
# @Author  :
# @Des     : any tools
# @File    : tools.py

import cx_Oracle
from datetime import datetime, timedelta


def delete_today_emulation():
    conn = cx_Oracle.connect("lklh/lklh@192.168.0.113/orcl")
    cur = conn.cursor()
    sql = "delete from tb_gps_simulate where speed_time >= :1 and speed_time < :2"
    now = datetime.now()
    td = datetime(now.year, now.month, now.day)
    et = td + timedelta(days=1)
    tup = (td, et)
    cur.execute(sql, tup)
    conn.commit()
    cur.close()
    conn.close()


def is_acc_off(data):
    state_off = '2'
    return data.state[-1] == state_off


def is_acc_on(data):
    state_on = '3'
    return data.state[-1] == state_on
