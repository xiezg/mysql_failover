
import time
import threading
from  mysql.connector.errors import OperationalError, DatabaseError,InterfaceError
import mysql.connector
import sys
import shutil
import os
import asyncio
import errno
import logging
import traceback

class MySQLMultiSourceUnSupport( mysql.connector.errors.Error ):
    def __init__(self):
        super().__init__( msg="暂不支持多源复制模式" )

class MySQLSlaveIOThreadException( mysql.connector.errors.Error):
    def __init__(self, errno, errmsg):
        super().__init__( msg=errmsg, errno=errno)

class MySQLSlaveSQLThreadException( mysql.connector.errors.Error):
    def __init__(self, errno, gtid, errmsg ):
        super().__init__( errno=errno, msg=errmsg)
        self.gtid = gtid

class MySQLGTIDInconsistencyException( mysql.connector.errors.Error):
    def __init__(self, errmsg):
        super().__init__( msg=errmsg )

class MySQLConnShutdown( mysql.connector.errors.Error ):
    pass

#slave角色信息丢失，不再是slave
class MySQLSlaveRoleMissException( mysql.connector.errors.Error):
    pass

