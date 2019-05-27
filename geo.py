# -*- coding: utf-8 -*-
# @Time    : 2018/11/2 11:01
# @Author  :
# @���    :
# @File    : geo.py
import numpy as np
from ctypes import *
from math import pi, sqrt, sin, cos
from sklearn.neighbors import KDTree
import time
dll = WinDLL("E:\job\jinniu\CoordTransDLL.dll")


class BLH(Structure):
    _fields_ = [("b", c_double),
                ("l", c_double),
                ("h", c_double)]


class XYZ(Structure):
    _fields_ = [("x", c_double),
                ("y", c_double),
                ("z", c_double)]


def bl2xy(b, l):
    """
    :param b: latitude
    :param l: longitude
    :return: x, y
    """
    blh = BLH()
    blh.b = float(b)
    blh.l = float(l)
    blh.h = 0
    xyz = XYZ()
    global dll
    dll.WGS84_BLH_2_HZ_xyH(blh, byref(xyz))
    y, x = xyz.x, xyz.y
    return x, y


def xy2bl(x, y):
    xyz = XYZ()
    blh = BLH()
    xyz.x, xyz.y, xyz.z = y, x, 0
    global dll
    dll.HZ_xyH_2_WGS84_BLH(xyz, byref(blh))
    return blh.b, blh.l


def calc_dist(pt0, pt1):
    v0 = np.array(pt0)
    v1 = np.array(pt1)
    dist = np.linalg.norm(v0 - v1)
    return dist


def point_project(point, segment_point0, segment_point1):
    """
    :param point: point to be matched [x, y]
    :param segment_point0: point [x0, y0]
    :param segment_point1: point [x1, y1]
    :return: projected point, state
            state 为1 在s0s1的延长线上
            state 为-1 在s1s0的延长线上
    """
    x, y = point[0:2]
    x0, y0 = segment_point0[0:2]
    x1, y1 = segment_point1[0:2]
    ap, ab = np.array([x - x0, y - y0]), np.array([x1 - x0, y1 - y0])
    ac = np.dot(ap, ab) / (np.dot(ab, ab)) * ab
    dx, dy = ac[0] + x0, ac[1] + y0
    state = 0
    if np.dot(ap, ab) < 0:
        state = -1
    bp, ba = np.array([x - x1, y - y1]), np.array([x0 - x1, y0 - y1])
    if np.dot(bp, ba) < 0:
        state = 1
    return [dx, dy], ac, state


def point2segment(point, segment_point0, segment_point1):
    """
    :param point: point to be matched, [px(double), py(double)]
    :param segment_point0: segment [px, py]
    :param segment_point1: [px, py]
    :return: dist from point to segment
    """
    x, y = point[0:2]
    x0, y0 = segment_point0[0:2]
    x1, y1 = segment_point1[0:2]
    cr = (x1 - x0) * (x - x0) + (y1 - y0) * (y - y0)
    if cr <= 0:
        return sqrt((x - x0) * (x - x0) + (y - y0) * (y - y0))
    d2 = (x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0)
    if cr >= d2:
        return sqrt((x - x1) * (x - x1) + (y - y1) * (y - y1))
    r = cr / d2
    px = x0 + (x1 - x0) * r
    py = y0 + (y1 - y0) * r
    return sqrt((x - px) * (x - px) + (y - py) * (y - py))


def segment_dist_position(segment_point0, segment_point1, dist):
    x0, y0 = segment_point0[0:2]
    x1, y1 = segment_point1[0:2]
    ab = np.array([x1 - x0, y1 - y0])
    total_dist = sqrt(np.dot(ab, ab))
    ap = ab / total_dist * dist
    dx, dy = ap[0] + x0, ap[1] + y0
    return [dx, dy]


def calc_included_angle(s0p0, s0p1, s1p0, s1p1):
    """
    计算夹角
    :param s0p0: 线段0点0 其中点用[x,y]表示
    :param s0p1: 线段0点1
    :param s1p0: 线段1点0
    :param s1p1: 线段1点1
    :return:
    """
    v0 = np.array([s0p1[0] - s0p0[0], s0p1[1] - s0p0[1]])
    v1 = np.array([s1p1[0] - s1p0[0], s1p1[1] - s1p0[1]])
    return np.dot(v0, v1) / (np.sqrt(np.dot(v0, v0)) * np.sqrt(np.dot(v1, v1)))


def calc_segment_coor(p0, p1, per):
    """
    计算线段上点p的坐标
    :param p0: 线段一段的坐标 [p0x, p0y]
    :param p1: 线段另一段的坐标 [p1x, p1y]
    :param per: 点p分线段p0,p1的比例
    :return: 点p的坐标
    """
    x = p0[0] + per * (p1[0] - p0[0])
    y = p0[1] + per * (p1[1] - p0[1])
    return x, y


# 84 to 02
a = 6378245.0
ee = 0.00669342162296594323
# World Geodetic System ==> Mars Geodetic System


def transform(wgLat, wgLon):
    """
    transform(latitude,longitude) , WGS84
    return (latitude,longitude) , GCJ02
    """
    dLat = transformLat(wgLon - 105.0, wgLat - 35.0)
    dLon = transformLon(wgLon - 105.0, wgLat - 35.0)
    radLat = wgLat / 180.0 * pi
    magic = sin(radLat)
    magic = 1 - ee * magic * magic
    sqrtMagic = sqrt(magic)
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi)
    dLon = (dLon * 180.0) / (a / sqrtMagic * cos(radLat) * pi)
    mgLat = wgLat + dLat
    mgLon = wgLon + dLon
    return mgLat, mgLon


def gcj02towgs84(lng, lat):
    """
    GCJ02(火星坐标系)转GPS84
    :param lng:火星坐标系的经度
    :param lat:火星坐标系纬度
    :return:
    """
    dlat = transformLat(lng - 105.0, lat - 35.0)
    dlng = transformLon(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [lng * 2 - mglng, lat * 2 - mglat]


def outOfChina(lat, lon):
    if lon < 72.004 or lon > 137.8347:
        return False
    if lat < 10.8293 or lat > 55.8271:
        return False
    return True


def transformLat(x, y):
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * sqrt(abs(x))
    ret += (20.0 * sin(6.0 * x * pi) + 20.0 * sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * sin(y * pi) + 40.0 * sin(y / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * sin(y / 12.0 * pi) + 320 * sin(y * pi / 30.0)) * 2.0 / 3.0
    return ret


def transformLon(x, y):
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * sqrt(abs(x))
    ret += (20.0 * sin(6.0 * x * pi) + 20.0 * sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * sin(x * pi) + 40.0 * sin(x / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * sin(x / 12.0 * pi) + 300.0 * sin(x / 30.0 * pi)) * 2.0 / 3.0
    return ret


def kDtree(xy_list, point):
    X = np.array(xy_list)
    # X = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]])
    kdt = KDTree(X, leaf_size=30, metric="euclidean")
    dis, ind = kdt.query(np.array(point).reshape(1, -1), k=1)
    return dis[0][0], ind[0][0]


def kDtree_query(kdt, point):
    dis, ind = kdt.query(np.array(point).reshape(1, -1), k=1)
    return dis[0][0], ind[0][0]
