import configparser
import os
from loguru import logger


class Config:
    def __init__(self):
        self.sqlIp_ = ""
        self.sqlPort_ = 0
        self.sqlUsername_ = ""
        self.sqlPassword_ = ""
        self.dataDir_ = ""

        self.readConfig()

    def toString(self):
        return "ip:{}, port:{}, username:{}, password:{}, dataDir:{}".format(self.sqlIp_, \
                                                                             self.sqlPort_, self.sqlUsername_,
                                                                             self.sqlPassword_, self.dataDir_)

    def readConfig(self):
        curDir = os.getcwd()
        configPath = os.path.join(curDir, "config.ini")
        logger.info("读取配置文件, 配置文件路径: {}", configPath)

        conf = configparser.ConfigParser()
        conf.read(configPath, encoding="utf8")
        self.sqlIp_ = conf.get("MySql", "ip", fallback="")
        self.sqlPort_ = conf.get("MySql", "port", fallback="")
        self.sqlUsername_ = conf.get("MySql", "username", fallback="")
        self.sqlPassword_ = conf.get("MySql", "password", fallback="")
        self.dataDir_ = conf.get("SqlData", "dataDir", fallback="")

        if self.sqlIp_ == "" or self.sqlPort_ == "" or self.sqlUsername_ == "" or \
                self.sqlPassword_ == "" or self.dataDir_ == "":
            raise Exception("配置文件错误")

        logger.info(self.toString())


gConfig = Config()
