# coding:utf8
import os
import pickle

from loguru import logger
from config import gConfig
import csv
import time
import datetime

gHistory = None
gLimit = 500000


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
    return strTime.strip()


def readOneRow(row):
    field = Field()
    try:
        field.__FILENAME__ = row[0].strip()
        field.__SOURCE__ = row[1].strip()
        field.__TIMESTAMP__ = float(row[2])
        field.localTime_ = convertTime(field.__TIMESTAMP__)
        field.__TOPICID__ = row[3].strip()
        field.__CONTENT__ = row[4].strip()
        field.LogParseFailure = row[5].strip()
        field.__TAG__container_name = row[6].strip()
        field.__TAG__namespace = row[7].strip()
        field.__TAG__pod_name = row[8].strip()
        field.host = row[9].strip()
        field.http_user_agent = row[10].strip()
        field.http_x_forwarded_for = row[11].strip()
        field.http_x_vlight_market = row[12].strip()
        field.http_x_vlight_os_name = row[13].strip()
        field.http_x_vlight_udid = row[14].strip()
        field.http_x_vlight_user_id = row[15].strip()
        field.http_x_vlight_version = row[16].strip()
        field.remote_addr = row[17].strip()
        field.request = row[18].strip()
        field.request_body = row[19].strip()
        field.request_time = row[20].strip()
        field.status = row[21].strip()
        field.upstream_status = row[22].strip()
    except:
        return None

    return field


def createIpInfo(filed):
    ip = IpInfo()
    ip.dictUa[filed.http_user_agent] = 1
    ip.dictStatus_[filed.status] = 1
    ip.dictUpstreamStatus_[filed.upstream_status] = 1
    ip.dictAppVersion_[filed.http_x_vlight_version] = 1
    ip.dictUserId_[filed.http_x_vlight_user_id] = 1
    ip.dictOsName_[filed.http_x_vlight_os_name] = 1
    ip.dictUdid_[filed.http_x_vlight_udid] = 1
    ip.dictMarket[filed.http_x_vlight_market] = 1
    ip.requestTotalTime_ = filed.request_time
    ip.firstReqTime_ = filed.__TIMESTAMP__
    ip.lastReqTime_ = filed.__TIMESTAMP__
    ip.requestCnts_ += 1
    return ip


def updateIpInfo(ip, filed):
    if filed.http_user_agent in ip.dictUa:
        ip.dictUa[filed.http_user_agent] += 1
    else:
        ip.dictUa[filed.http_user_agent] = 1

    if filed.status in ip.dictStatus_:
        ip.dictStatus_[filed.status] += 1
    else:
        ip.dictStatus_[filed.status] = 1

    if filed.upstream_status in ip.dictUpstreamStatus_:
        ip.dictUpstreamStatus_[filed.upstream_status] += 1
    else:
        ip.dictUpstreamStatus_[filed.upstream_status] = 1

    if filed.http_x_vlight_version in ip.dictAppVersion_:
        ip.dictAppVersion_[filed.http_x_vlight_version] += 1
    else:
        ip.dictAppVersion_[filed.http_x_vlight_version] = 1

    if filed.http_x_vlight_user_id in ip.dictUserId_:
        ip.dictUserId_[filed.http_x_vlight_user_id] += 1
    else:
        ip.dictUserId_[filed.http_x_vlight_user_id] = 1

    if filed.http_x_vlight_os_name in ip.dictOsName_:
        ip.dictOsName_[filed.http_x_vlight_os_name] += 1
    else:
        ip.dictOsName_[filed.http_x_vlight_os_name] = 1

    if filed.http_x_vlight_udid in ip.dictUdid_:
        ip.dictUdid_[filed.http_x_vlight_udid] += 1
    else:
        ip.dictUdid_[filed.http_x_vlight_udid] = 1

    if filed.http_x_vlight_market in ip.dictMarket:
        ip.dictMarket[filed.http_x_vlight_market] += 1
    else:
        ip.dictMarket[filed.http_x_vlight_market] = 1

    ip.requestTotalTime_ += filed.request_time

    if filed.__TIMESTAMP__ < ip.firstReqTime_:
        ip.firstReqTime_ = filed.__TIMESTAMP__

    if filed.__TIMESTAMP__ > ip.lastReqTime_:
        ip.lastReqTime_ = filed.__TIMESTAMP__

    ip.requestCnts_ += 1


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

        if "?" in p or "." in p or "%" in p or '-' in p:
            # 参数部分
            break

        if p == "":
            fullHeader += "/"
        else:
            # 判断有没有可能是md5
            if len(p) == 32 or len(p) == 24 or len(p) == 20:
                fullHeader += "xxx/"
                # isMd5 = True
                # for c in p:
                #     if not (('0' <= c <= '9') or ('a' <= c <= 'f') or ('A' <= c <= 'F')):
                #         isMd5 = False
                #         break
                #
                # if isMd5:
                #     fullHeader += "xxx/"
                # else:
                #     fullHeader += p + "/"
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


def newRequest(fullHeader, ip, filed):
    global gHistory
    ri = RequestInfo()
    ri.host_ = filed.host
    ri.header_ = fullHeader
    ri.dictIp_[filed.remote_addr] = ip
    gHistory.dictRequest_[fullHeader] = ri
    ri.requestCnts_ += 1
    gHistory.dictHR_[fullHeader] = filed.host
    return ri


def readCsv(fullPath):
    global gHistory
    global gLimit

    # 判断文件是否处理过, 可能因为上次异常中断
    if fullPath in gHistory.dictHandleFiles_:
        useTime = gHistory.dictHandleFiles_[fullPath]
        logger.warning("已经处理过, 上次处理耗时:{}, 文件:{}, ", useTime, fullPath)
        return

    fileSize = round(os.path.getsize(fullPath) / 1024 / 1024 / 1024, 2)
    logger.info("开始处理文件: {}, 大小:{}GB", fullPath, fileSize)
    fileCnt_ = 0
    startTime = time.time()
    with open(fullPath, 'r', newline='', encoding='utf8') as f:
        reader = csv.reader(f)

        fileHandleCnt = 0
        limitStartTime = time.time()

        for row in reader:

            if row[0].startswith("__"):
                continue

            filed = readOneRow(row)
            if filed is None:
                continue

            fullHeader = extratHeader(filed.request).strip()
            if fullHeader == "":
                continue

            ip = createIpInfo(filed)

            # 查找该请求是否存在
            if fullHeader in gHistory.dictRequest_:
                ri = gHistory.dictRequest_[fullHeader]
                # 判断ip是否存在
                if filed.remote_addr in ri.dictIp_:
                    ip = ri.dictIp_[filed.remote_addr]
                    updateIpInfo(ip, filed)
                    ri.requestCnts_ += 1
                else:
                    ri = newRequest(fullHeader, ip, filed)
            else:
                ri = newRequest(fullHeader, ip, filed)

            gHistory.handleCnt_ += 1
            fileCnt_ += 1
            fileHandleCnt += 1
            ip = filed.remote_addr.strip()

            if ip in gHistory.dictIp_:
                gHistory.dictIp_[ip] += 1
            else:
                gHistory.dictIp_[ip] = 1

            if fileHandleCnt >= gLimit:
                limitUseTime = time.time() - limitStartTime
                logger.info("处理:{}条数据, 单次耗时:{}, url分类总个数:{}, 访问ip总个数:{}, 总处理条数:{}", fileHandleCnt,
                            round(limitUseTime, 2),
                            len(gHistory.dictRequest_), len(gHistory.dictIp_), gHistory.handleCnt_)
                fileHandleCnt = 0
                limitStartTime = time.time()

    gHistory.handleSize_ += fileSize
    useTime = time.time() - startTime
    logger.warning("处理文件:{}, 文件总计耗时:{}, 该文件处理条数:{}, 总处理条数:{}, 总处理文件大小:{}GB", fullPath, round(useTime, 2), fileCnt_,
                gHistory.handleCnt_,
                round(gHistory.handleSize_, 2))

    gHistory.totalProgressTime_ += useTime
    gHistory.dictHandleFiles_[fullPath] = useTime

    saveProgress()


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
    global gHistory
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

    logger.error("程序结束, 单次运行耗时:{}, 总计运行耗时:{}", round(useTime, 2), round(time.time() - gHistory.startTime_, 2))


if __name__ == '__main__':
    main()
