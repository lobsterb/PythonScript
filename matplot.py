# coding:utf8
import pickle
import time
import datetime

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pickle as pl

from loguru import logger

mpl.rcParams["font.sans-serif"]=["SimHei"]
mpl.rcParams["axes.unicode_minus"]=False

gHistory = None


class IpInfo:

    def __init__(self):
        # ua <str, int>
        self.dictUa = {}
        # 请求返回状态 <str, int>
        self.dictStatus_ = {}
        # 反向代理状态 <str, int>
        self.dictUpstreamStatus_ = {}
        # app版本 <str, int>
        self.dictAppVersion_ = {}
        # 业务用户id <str, int>
        self.dictUserId_ = {}
        # 客户端系统名称 <str, int>
        self.dictOsName_ = {}
        # 客户设备id <str, int>
        self.dictUdid_ = {}
        # 客户端渠道 <str, int>
        self.dictMarket = {}
        # 请求总时长
        self.requestTotalTime_ = 0
        # 第一个请求时间
        self.firstReqTime_ = ""
        # 最后一个请求时间 = ""
        self.lastReqTime_ = ""
        # 访问次数
        self.requestCnts_ = 0


class RequestInfo:
    def __init__(self):
        self.host_ = ""
        # 请求头
        self.header_ = ""
        # ip地址 <str, IpInfo>
        self.dictIp_ = {}
        # 总计访问次数
        self.requestCnts_ = 0


class HandleHistory:
    def __init__(self):
        # 已经处理过的文件
        self.dictHandleFiles_ = {}
        self.totalProgressTime_ = 0
        self.handleCnt_ = 0
        # 所有host <requestHeader, Request>
        self.dictRequest_ = {}
        self.dictHR_ = {}
        self.dictIp_ = {}
        self.startTime_ = time.time()
        self.handleSize_ = 0.0


def readProgress():
    global gHistory

    try:
        output = open('history', 'rb')
        gHistory = pickle.load(output)
        return
    except Exception as e:
        logger.warning(str(e))

    gHistory = HandleHistory()
    return


def convertTime(unixTime):
    t = int(unixTime / 1000)
    t = datetime.datetime.fromtimestamp(t)
    strTime = t.strftime("%Y-%m-%d %H:%M:%S")
    return strTime.strip()


def initLog():
    logger.add("logs/{time:YYYY-MM-DD}-Statistics.log", rotation="5 MB",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")


def main():
    global gHistory
    initLog()

    startTime = time.time()

    # 读取历史记录
    readProgress()

    # 引入数据
    x = [1, 2, 3, 4, 5, 6]
    y = [6, 10, 4, 5, 1, 2]

    dictRequest = gHistory.dictRequest_

    # 查找访问最高的20个url
    n = 0
    dictTop = {}
    for k, v in dictRequest.items():
        dictTop[k] = v.requestCnts_

        if "message/copy" in k:
            request = v


        if v.requestCnts_ > n:
            n = v.requestCnts_

    print(sorted(dictTop.items(), key = lambda x:x[1], reverse=True))

    x = []
    y = []
    tickLabel = []
    idx = 1
    for k, v in dictRequest.items():
        if "$" in k:
            continue

        x.append(idx)
        y.append(v.requestCnts_)
        tickLabel.append(k)
        idx += 1
        if idx > 20:
            break


    # 构造柱状图参数
    #plt.bar(x, y, align='center', color="b", tick_label=['A', "B", "C", "D", "E", "F"], alpha=0.6)
    plt.barh(x, y, align='center', color="b", tick_label=tickLabel, alpha=0.6)

    # 设置横纵坐标
    plt.xlabel('url路由路径')
    plt.ylabel('访问次数')
    plt.grid(True, axis="y", ls=":", color="r", alpha=0.3)
    plt.show()

    endTime = time.time()
    useTime = endTime - startTime

    logger.error("程序结束, 单次运行耗时:{}, 总计运行耗时:{}", round(useTime, 2), round(time.time() - gHistory.startTime_, 2))


if __name__ == '__main__':
    main()
