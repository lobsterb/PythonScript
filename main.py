# coding:utf8
import os
import mysql.connector

from loguru import logger
from config import gConfig
import csv
import time

myDb = mysql.connector.connect(
    host=gConfig.sqlIp_,
    user=gConfig.sqlUsername_,
    passwd=gConfig.sqlPassword_,
    auth_plugin='mysql_native_password',
    database='qingqi1'
)
myDb.autocommit = 1
gErrorCount = 0


class Field:
    def __init__(self):
        self.__FILENAME__ = ""
        self.__SOURCE__ = ""
        self.__TIMESTAMP__ = ""
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


def initLog():
    logger.add("logs/{time:YYYY-MM-DD}.log", rotation="5 MB", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")


def saveToDb(listField, filePath, isFinish=False):
    global gErrorCount
    tblName = "normal"
    if "提取异常ip日志数据" in filePath:
        tblName = "exception"

    # insertSql = "INSERT INTO `{}` VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{})"

    insertSql = "INSERT INTO `{}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insertSql = insertSql.format(tblName)

    startTime = time.time()
    cursor = myDb.cursor()
    cursor.execute("BEGIN;")
    for field in listField:

        # var = "'" + field.__FILENAME__ + "'," + \
        #       "'" + field.__SOURCE__ + "'," + \
        #       "'" + field.__TIMESTAMP__ + "'," + \
        #       "'" + field.__TOPICID__ + "'," + \
        #       "'" + field.__CONTENT__ + "'," + \
        #       "'" + field.LogParseFailure + "'," + \
        #       "'" + field.__TAG__container_name + "'," + \
        #       "'" + field.__TAG__namespace + "'," + \
        #       "'" + field.__TAG__pod_name + "'," + \
        #       "'" + field.host + "'," + \
        #       "'" + field.http_user_agent + "'," + \
        #       "'" + field.http_x_forwarded_for + "'," + \
        #       "'" + field.http_x_vlight_market + "'," + \
        #       "'" + field.http_x_vlight_os_name + "'," + \
        #       "'" + field.http_x_vlight_udid + "'," + \
        #       "'" + field.http_x_vlight_user_id + "'," + \
        #       "'" + field.http_x_vlight_version + "'," + \
        #       "'" + field.remote_addr + "'," + \
        #       "'" + field.request + "'," + \
        #       "'" + field.request_body + "'," + \
        #       "'" + field.request_time + "'," + \
        #       "'" + field.status + "'," + \
        #       "'" + field.upstream_status + "'"

        try:
            bBody = bytes(field.request_body, encoding="utf8")

            values = (field.__FILENAME__, field.__SOURCE__, field.__TIMESTAMP__,
                      field.__TOPICID__,
                      field.__CONTENT__, field.LogParseFailure, field.__TAG__container_name,
                      field.__TAG__namespace,
                      field.__TAG__pod_name, field.host, field.http_user_agent, field.http_x_forwarded_for,
                      field.http_x_vlight_market,
                      field.http_x_vlight_os_name, field.http_x_vlight_udid, field.http_x_vlight_user_id,
                      field.http_x_vlight_version,
                      field.remote_addr, field.request, mysql.connector.Binary(field.request_body, encoding="utf8"),
                      field.request_time,
                      field.status, field.upstream_status);

            # sql = insertSql.format(tblName, field.__FILENAME__, field.__SOURCE__, field.__TIMESTAMP__,
            #                        field.__TOPICID__,
            #                        field.__CONTENT__, field.LogParseFailure, field.__TAG__container_name,
            #                        field.__TAG__namespace,
            #                        field.__TAG__pod_name, field.host, field.http_user_agent, field.http_x_forwarded_for,
            #                        field.http_x_vlight_market,
            #                        field.http_x_vlight_os_name, field.http_x_vlight_udid, field.http_x_vlight_user_id,
            #                        field.http_x_vlight_version,
            #                        field.remote_addr, field.request, mysql.connector.Binary(field.request_body, encoding="utf8"), field.request_time,
            #                        field.status, field.upstream_status)

            # sql = insertSql.format(tblName, var)
            if '__FILENAME__' in field.__FILENAME__:
                continue
            cursor.execute(insertSql, values)
        except Exception as e:
            gErrorCount += 1
            logger.error(str(e) + ":" + str(gErrorCount))

    myDb.commit()
    endTime = time.time()
    logger.info("入库:{}条数据, 耗时:{}", len(listField), endTime - startTime)

    # 查询
    searchSql = "SELECT filePath, readRow FROM `readcsvrecord` where filePath = '{}'"
    cursor = myDb.cursor()
    sql = searchSql.format(filePath)
    cursor.execute(sql)
    result = cursor.fetchall()
    if len(result) > 0:
        readRow = result[0][1]
        readRow += len(listField)
        fileRow = 0
        if isFinish:
            fileRow = readRow

        updateSql = "UPDATE `readcsvrecord` SET readRow = {}, fileRow = {} where filePath = '{}';"
        sql = updateSql.format(readRow, fileRow, filePath)
        cursor = myDb.cursor()
        cursor.execute(sql)
        myDb.commit()
    else:
        insertSql = "INSERT DELAYED INTO `readcsvrecord` VALUES ({});"
        var = "'" + filePath + "'," + str(len(listField)) + "," + str(0)
        sql = insertSql.format(var)
        cursor = myDb.cursor()
        cursor.execute(sql)
        myDb.commit()


def readCsv(filePath):
    # 判断从哪里开始读取
    searchSql = "SELECT filePath, readRow, fileRow FROM `readcsvrecord` where filePath = '{}'"
    cursor = myDb.cursor()
    sql = searchSql.format(filePath)
    cursor.execute(sql)
    result = cursor.fetchall()

    startIndex = 0
    if len(result) > 0:
        readRow = result[0][1]
        fileRow = result[0][2]

        if readRow != 0 and readRow == fileRow:
            logger.info("文件已经入库, {}", filePath)
            return

        startIndex = readRow

    listField = []
    limit = 100000

    logger.info("读取csv文件:{}", filePath)
    with open(filePath, 'r', newline='', encoding='utf8') as f:
        reader = csv.reader(f)

        startTime = time.time()
        curRow = 0
        for row in reader:

            if curRow < startIndex:
                curRow += 1
                continue

            cellSize = len(row)
            if cellSize != 23:
                logger.error("数据长度错误,{}", row)
            else:
                field = Field()
                field.__FILENAME__ = row[0]
                field.__SOURCE__ = row[1]
                s = row[2]
                if not s.startswith(("__")):
                    field.__TIMESTAMP__ = str(int(float(row[2])))
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
                listField.append(field)

                if len(listField) >= limit:
                    endTime = time.time()
                    logger.info("读取{}条记录耗时:{}", limit, endTime - startTime)

                    saveToDb(listField, filePath)
                    listField = []
                    startTime = time.time()
            curRow += 1

        # 写入最后的数据
        if len(listField) > 0:
            saveToDb(listField, filePath, True)


def TraverseDir(rootDir):
    listDir = os.listdir(rootDir)
    for path in listDir:
        fullPath = os.path.join(rootDir, path)
        if os.path.isdir(fullPath):
            TraverseDir(fullPath)
        else:
            readCsv(fullPath)


def initDb():
    cursor = myDb.cursor()
    cursor.execute("SHOW TABLES")

    listTable = []
    for item in cursor:
        listTable.append(item[0])

    if 'exception' not in listTable:
        sql = """
                      CREATE TABLE `exception`  (
                      `__FILENAME__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__SOURCE__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TIMESTAMP__` BIGINT(50) NOT NULL,
                      `__TOPICID__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__CONTENT__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `LogParseFailure` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TAG__.container_name` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TAG__.namespace` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TAG__.pod_name` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `host` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_user_agent` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_forwarded_for` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_market` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_os_name` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_udid` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_user_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_version` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `remote_addr` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `request` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `request_body` BLOB DEFAULT NULL,
                      `request_time` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `status` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `upstream_status` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
                    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic PARTITION BY HASH(__TIMESTAMP__) PARTITIONS 1000;
                """
        cursor.execute(sql)

    if 'normal' not in listTable:
        sql = """
                      CREATE TABLE `normal`  (
                      `__FILENAME__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__SOURCE__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TIMESTAMP__` BIGINT(50) NOT NULL,
                      `__TOPICID__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__CONTENT__` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `LogParseFailure` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TAG__.container_name` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TAG__.namespace` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `__TAG__.pod_name` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `host` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_user_agent` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_forwarded_for` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_market` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_os_name` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_udid` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_user_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `http_x_vlight_version` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `remote_addr` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `request` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `request_body` BLOB DEFAULT NULL,
                      `request_time` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `status` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
                      `upstream_status` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL
                    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic PARTITION BY HASH(__TIMESTAMP__) PARTITIONS 1000;
                """
        cursor.execute(sql)

    if 'readcsvrecord' not in listTable:
        sql = """
        CREATE TABLE `readcsvrecord`  (
        `filePath` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
        `readRow` int(0) NOT NULL,
        `fileRow` int(0) NOT NULL
        ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
        """
        cursor.execute(sql)


def main():
    initLog()
    logger.info(gConfig)

    # 初始化数据库
    initDb()

    # 遍历目录
    rootDir = gConfig.dataDir_
    TraverseDir(rootDir)


if __name__ == '__main__':
    main()
