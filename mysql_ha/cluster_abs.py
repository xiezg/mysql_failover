from abc import ABC, abstractmethod
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
from .errors import *

logger = logging.getLogger()

class MySQLDbAbstract(ABC):
    pass

class MySQLHATopologyAbstract(ABC):
    @abstractmethod
    def start( self, auto_failover, call_back ):
        pass

    @abstractmethod
    def stop( self ):
        pass
#TODO
#slave sql/thread在执行错误时.执行的对象是什么？(1、事物：)
    #binlog的格式
#slave io/thread在执行错误时.
class MySQLSingleMasterSingleSlaveCluster(MySQLHATopologyAbstract):

    def slave_maintain(self):
        try:
            if not self.slave.is_slave_io_thread_running():
                logger.warning( "slave io_thread not running" )

            if not self.slave.is_slave_sql_thread_running():
                logger.warning( "slave sql_thread not running" ) #slave应该被正常stop slave; 这种情况不进行干预
        except MySQLSlaveIOThreadException as e:
            logger.error( e )
            if e.errno == 2003:
                logger.error( e.msg )
                return
            elif e.errno == 1236:
                self.slave.gtid_force_sync(self.master )
                self.slave.restart_slave()
            elif e.errno == 2013:
                logger.warning( "slave can't connect to master {}".format(e.msg) )
                return
            else:
                raise e
        except MySQLSlaveSQLThreadException as e:
            logger.error(e)
            if e.errno == 1008:
                #Can't drop database 't11'; database doesn't exist' on query. Default database: 't11'. Query: 'drop database t11'
                self.slave.skip_transactions_with_gtid( e.gtid )
                return

            if e.errno == 1051:
                #Unknown table 't12.t'' on query. Default database: 't12'. Query: 'DROP TABLE `t`
                self.slave.skip_transactions_with_gtid( e.gtid )
                return

            if e.errno == 1050:
                #创建的表已经存在
                #解决方案: 将该表重命名，并重启复制动作
                return

            if e.errno == 1060:
                #修改表结构，给表添加列，该列已经存在
                #errmsg: Error 1060: Duplicate column name
                #解决方案：？
                return

            if e.errno == 1007:
                #'Can't create database 't'; database exists' on query
                self.slave.skip_transactions_with_gtid( e.gtid )
                return

            if e.errno == 1032: 
                #Could not execute Delete_rows event on table t.t; 
                #Can't find record in 't', Error_code: 1032
                self.slave.skip_transactions_with_gtid( e.gtid )
                return

            if e.errno == 1146: 
                #Error executing row event: 'Table 't13.t' doesn't exist'
                #操作某个表时，该表不存在
                #pass
                self.slave.skip_transactions_with_gtid( e.gtid )
                return

            if e.errno == 1677:
                #Column 1 of table 'x.t' cannot be converted 
                #from type 'varchar(100(bytes))' to type 'varchar(200(bytes) latin1
                #主从表数据类型不一致
                #pass    #目前不支持自动修复
                return

            raise e

