# -*- coding: utf-8 -*-
# @Time    : 2018/11/2 11:01
# @Author  :
# @File    : struct_808.py

from geo import calc_included_angle, point2segment, calc_dist


# VehiData
# 保存车辆信息

class VehiData:
    def __init__(self, lng, lat, x, y, alt, speed, speed_time, veh_no, last_data, alarm, state, orient, carstate):
        self.lng, self.lat, self.alt, self.speed_time, self.veh_no = lng, lat, alt, speed_time, veh_no
        # 经度 纬度 高度 时间 车牌
        self.x, self.y, self.speed = x, y, speed
        # 坐标xy 速度d
        self.last_data = last_data
        # 上一个数据，用于判断是否补数据
        self.orient = orient        # 方向 angle
        self.alarm, self.state = alarm, state
        self.line_index = -1        # 落在哪条线段上
        self.carstate = carstate

    def __sub__(self, other):
        return (self.speed_time - other.speed_time).total_seconds()

    def __lt__(self, other):
        return self.speed_time < other.speed_time

    def pass_day(self):
        if self.last_data.speed_time.day != self.speed_time.day:
            return True

    def lost(self):
        """
        :return: 是否丢失数据
        """
        if self.last_data is None:
            return False
        last_point = [self.last_data.x, self.last_data.y]
        cur_point = [self.x, self.y]
        dist = calc_dist(last_point, cur_point)
        if dist < 5000:        # 小于5公里不补
            return False
        return (self.speed_time - self.last_data.speed_time).total_seconds() > 120

    def set_last_data(self, data):
        self.last_data = data

    def near_z(self, line):
        z_point = line[0]
        z_dist = calc_dist(z_point, [self.x, self.y])
        if z_dist < 10000:
            return True
        else:
            return False

    def near_f(self, line):
        f_point = line[-1]
        f_dist = calc_dist(f_point, [self.x, self.y])
        if f_dist < 10000:
            return True
        else:
            return False

    def judge_orient(self, line):
        """
        :param line:
        :return:
        """
        if self.last_data is None:
            return 'n'
        if self.speed == 0 and self.last_data.speed == 0:
            return 'n'
        if self.last_data.x == self.x and self.last_data.y == self.y:
            return 'n'

        z_point, f_point = line[0], line[-1]
        last_point, cur_point = [self.last_data.x, self.last_data.y], [self.x, self.y]
        angle = calc_included_angle(last_point, cur_point, z_point, f_point)

        if angle > 0:
            return 'z'
        else:
            return 'f'

    def nearest_line(self, line):
        """
        :param line:
        :return: nearest dist from line, and line index
        """
        min_dist, idx = 1e10, -1
        cur_point = [self.x, self.y]
        for i, pt in enumerate(line):
            if i > 0:
                lpt = line[i - 1]
                dist = point2segment(cur_point, lpt, pt)
                if dist < min_dist:
                    min_dist, idx = dist, i - 1
        return min_dist, idx

    def judge_line(self, line):
        if self.line_index == -1:
            dist, idx = self.nearest_line(line)
            if dist < 5000:     # 在道路内
                self.line_index = idx


class VehiOrientInfo:
    # 存储某辆车的方向记录
    def __init__(self):
        self.orient = None
        self.judge_list = []

    def add_judge(self, judge):
        """
        增加一次判断，并检查是否确定道路
        :param judge: 'z' or 'f'
        'z' MG-HZ  'f' HZ-MG
        :return:
        """
        if self.orient is not None:
            return
        self.judge_list.append(judge)
        self.check()
        # print self.orient

    def check(self):
        cnt0, cnt1 = self.judge_list.count('z'), self.judge_list.count('f')
        if cnt0 > 3 or cnt1 > 3:
            rate = 1.0 * cnt0 / (cnt0 + cnt1)
            if rate >= 0.75:
                self.orient = 'z'
            elif rate <= 0.25:
                self.orient = 'f'

    def clear(self):
        self.orient = None
        self.judge_list = []
