# coding:utf8
import os
import pickle

from loguru import logger
from config import gConfig
import csv
import time
import datetime

gHistory = None


class RequestInfo:
    def __init__(self):
        # 请求头
        self.header_ = ""
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


class HostInfo:
    def __init__(self):
        self.host_ = ""
        # <reqHeader, RequestInfo>
        self.dictRequests_ = {}
        # 请求类别总数
        self.totalRequests_ = 0


class HandleHistory:
    def __init__(self):
        # 已经处理过的文件
        self.dictHandleFiles_ = {}
        self.totalProgressTime_ = 0
        self.handleCnt_ = 0
        # 所有host <ip, host>
        self.dictHost_ = {}
        # host类别总数
        self.totalHost_ = 0


def saveProgress():
    global gHistory
    output = open('history', 'wb')
    pickle.dump(gHistory, output)


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


class Field:
    def __init__(self):
        self.__FILENAME__ = ""
        self.__SOURCE__ = ""
        self.__TIMESTAMP__ = ""
        self.localTime_ = ""
        self.__TOPICID__ = ""
        self.__CONTENT__ = ""
        self.LogParseFailure = ""
        self.__TAG__container_name = ""
        self.__TAG__namespace = ""
        self.__TAG__pod_name = ""
        self.host = ""
        self.http_user_agent = ""
        self.http_x_forwarded_for = ""
        self.http_x_vlight_market = ""
        self.http_x_vlight_os_name = ""
        self.http_x_vlight_udid = ""
        self.http_x_vlight_user_id = ""
        self.http_x_vlight_version = ""
        self.remote_addr = ""
        self.request = ""
        self.request_body = ""
        self.request_time = ""
        self.status = ""
        self.upstream_status = ""


def convertTime(unixTime):
    t = int(unixTime / 1000)
    t = datetime.datetime.fromtimestamp(t)
    strTime = t.strftime("%Y-%m-%d %H:%M:%S")
    return strTime


def readOneRow(row):
    field = Field()
    field.__FILENAME__ = row[0]
    field.__SOURCE__ = row[1]
    s = row[2]
    if s.startswith(("__")):
        return None
    field.__TIMESTAMP__ = float(float(row[2]))
    field.localTime_ = convertTime(field.__TIMESTAMP__)
    field.__TOPICID__ = row[3]
    field.__CONTENT__ = row[4]
    field.LogParseFailure = row[5]
    field.__TAG__container_name = row[6]
    field.__TAG__namespace = row[7]
    field.__TAG__pod_name = row[8]
    field.host = row[9]
    field.http_user_agent = row[10]
    field.http_x_forwarded_for = row[11]
    field.http_x_vlight_market = row[12]
    field.http_x_vlight_os_name = row[13]
    field.http_x_vlight_udid = row[14]
    field.http_x_vlight_user_id = row[15]
    field.http_x_vlight_version = row[16]
    field.remote_addr = row[17]
    field.request = row[18]
    field.request_body = row[19]
    field.request_time = row[20]
    field.status = row[21]
    field.upstream_status = row[22]
    return field


def createRequestInfo(filed):
    ri = RequestInfo()
    ri.header_ = filed.request
    ri.dictUa[filed.http_user_agent] = 1
    ri.dictStatus_[filed.status] = 1
    ri.dictUpstreamStatus_[filed.upstream_status] = 1
    ri.dictAppVersion_[filed.http_x_vlight_version] = 1
    ri.dictUserId_[filed.http_x_vlight_user_id] = 1
    ri.dictOsName_[filed.http_x_vlight_os_name] = 1
    ri.dictUdid_[filed.http_x_vlight_udid] = 1
    ri.dictMarket[filed.http_x_vlight_market] = 1
    ri.requestTotalTime_ = filed.request_time
    ri.firstReqTime_ = filed.__TIMESTAMP__
    ri.lastReqTime_ = filed.__TIMESTAMP__
    return ri


def updateRequestInfo(ri, filed):
    if filed.http_user_agent in ri.dictUa:
        ri.dictUa[filed.http_user_agent] += 1
    else:
        ri.dictUa[filed.http_user_agent] = 1

    if filed.status in ri.dictStatus_:
        ri.dictStatus_[filed.status] += 1
    else:
        ri.dictStatus_[filed.status] = 1

    if filed.upstream_status in ri.dictUpstreamStatus_:
        ri.dictUpstreamStatus_[filed.upstream_status] += 1
    else:
        ri.dictUpstreamStatus_[filed.upstream_status] = 1

    if filed.http_x_vlight_version in ri.dictAppVersion_:
        ri.dictAppVersion_[filed.http_x_vlight_version] += 1
    else:
        ri.dictAppVersion_[filed.http_x_vlight_version] = 1

    if filed.http_x_vlight_user_id in ri.dictUserId_:
        ri.dictUserId_[filed.http_x_vlight_user_id] += 1
    else:
        ri.dictUserId_[filed.http_x_vlight_user_id] = 1

    if filed.http_x_vlight_os_name in ri.dictOsName_:
        ri.dictOsName_[filed.http_x_vlight_os_name] += 1
    else:
        ri.dictOsName_[filed.http_x_vlight_os_name] = 1

    if filed.http_x_vlight_udid in ri.dictUdid_:
        ri.dictUdid_[filed.http_x_vlight_udid] += 1
    else:
        ri.dictUdid_[filed.http_x_vlight_udid] = 1

    if filed.http_x_vlight_market in ri.dictMarket:
        ri.dictMarket[filed.http_x_vlight_market] += 1
    else:
        ri.dictMarket[filed.http_x_vlight_market] = 1

    ri.requestTotalTime_ += filed.request_time

    if filed.__TIMESTAMP__ < ri.firstReqTime_:
        ri.firstReqTime_ = filed.__TIMESTAMP__

    if filed.__TIMESTAMP__ > ri.lastReqTime_:
        ri.lastReqTime_ = filed.__TIMESTAMP__


def extratHeader(request):
    fullHeader = ""
    requestHeader = request

    posHTTP = requestHeader.rfind("HTTP")
    if posHTTP < 0:
        return fullHeader

    posFirstForwardSlash = requestHeader.find("/")
    if posFirstForwardSlash < 0:
        return fullHeader

    posLastForwardSlash = requestHeader.rfind("/", 0, posHTTP)
    if posLastForwardSlash < 0:
        return fullHeader

    action = requestHeader[0:posFirstForwardSlash]
    fullHeader = action
    if posFirstForwardSlash == posLastForwardSlash:
        # 访问的就是根目录
        fullHeader += "/"
        return fullHeader

    api = requestHeader[posFirstForwardSlash: posHTTP]
    vecPeaces = api.split("/")

    for p in vecPeaces:
        p = p.strip()

        if "?" in p or "." in p:
            # 参数部分
            break

        if p == "":
            fullHeader += "/"
        else:
            # 判断有没有可能是md5
            if len(p) == 32:
                isMd5 = True
                for c in p:
                    if not (('0' <= c <= '9') or ('a' <= c <= 'f') or ('A' <= c <= 'F')):
                        isMd5 = False
                        break

                if isMd5:
                    fullHeader += "xxx/"
                else:
                    fullHeader += p + "/"
            else:
                # 检测是否是纯数字
                isNumber = True
                for c in p:
                    if not ('0' <= c <= '9'):
                        isNumber = False
                        break

                if isNumber:
                    fullHeader += "xxx/"
                else:
                    fullHeader += p + "/"

    return fullHeader


def readCsv(fullPath):
    global gHistory

    # 判断文件是否处理过, 可能因为上次异常中断
    if fullPath in gHistory.dictHandleFiles_:
        useTime = gHistory.dictHandleFiles_[fullPath]
        logger.warning("已经处理过, 上次处理耗时:{}, 文件:{}, ", useTime, fullPath)
        return

    logger.info("开始处理文件: {}, 大小:{}", fullPath, os.path.getsize(fullPath) / 1024 / 1024)
    fileCnt_ = 0
    startTime = time.time()
    with open(fullPath, 'r', newline='', encoding='utf8') as f:
        reader = csv.reader(f)
        for row in reader:
            filed = readOneRow(row)
            if filed is None:
                continue
            gHistory.handleCnt_ += 1
            fileCnt_ += 1

            fullHeader = extratHeader(filed.request)
            if fullHeader == "":
                continue

            # filed.remote_addr = "127.0.0.1"

            # 查找该IP是否存在访问记录
            if filed.remote_addr in gHistory.dictHost_:
                hostInfo = gHistory.dictHost_[filed.remote_addr]
                # 判断该请求是否存在
                if fullHeader in hostInfo.dictRequests_:
                    ri = hostInfo.dictRequests_[fullHeader]
                    updateRequestInfo(ri, filed)
                else:
                    ri = createRequestInfo(filed)
                    hostInfo.dictRequests_[fullHeader] = ri
                hostInfo.totalRequests_ += 1
            else:
                # 创建一个新的ip访问记录
                hostInfo = HostInfo()
                hostInfo.host_ = filed.host
                ri = createRequestInfo(filed)
                hostInfo.dictRequests_[fullHeader] = ri
                gHistory.dictHost_[filed.remote_addr] = hostInfo
                hostInfo.totalRequests_ += 1

            # logger.info("ip:{}, host:{}, req:{}, 处理数据:{}条", filed.remote_addr, filed.host, fullHeader,
            #             hostInfo.totalRequests_)

    endTime = time.time()

    useTime = endTime - startTime
    logger.info("处理文件:{}, 耗时:{}, 该文件处理条数:{}, 总条数:{}", fullPath, useTime, fileCnt_, gHistory.handleCnt_)

    gHistory.totalProgressTime_ += useTime
    gHistory.dictHandleFiles_[fullPath] = useTime

    saveProgress()
    exit(0)


def TraverseDir(rootDir):
    listDir = os.listdir(rootDir)
    for path in listDir:
        fullPath = os.path.join(rootDir, path)
        if os.path.isdir(fullPath):
            TraverseDir(fullPath)
        else:
            readCsv(fullPath)


def initLog():
    logger.add("logs/{time:YYYY-MM-DD}-Statistics.log", rotation="5 MB",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")


def main():
    initLog()
    logger.info(gConfig)

    startTime = time.time()

    # 读取历史记录
    readProgress()

    # 遍历目录
    rootDir = gConfig.dataDir_
    TraverseDir(rootDir)

    endTime = time.time()
    useTime = endTime - startTime

    logger.info("程序结束, 总共耗时:{}", useTime)


if __name__ == '__main__':
    main()
