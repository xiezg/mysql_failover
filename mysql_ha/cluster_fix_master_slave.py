###### python 3.8.16
###### mysql-connector-python 8.0.32

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
from .cluster_abs import MySQLSingleMasterSingleSlaveCluster
from .errors import MySQLConnShutdown
from .mysql_db import MySQLDb

logger = logging.getLogger()

#固定主节点的主从数据库集群
class MySQLFixMasterSlaveCluster(MySQLSingleMasterSingleSlaveCluster):
    def __init__( self, master, slave, init ):
        if init :
            master.gtid_clean()
            slave.gtid_clean()

        self.__work_t1 = None

        #强制切换节点角色
        master.switch_to_master()
        slave.switch_to_slave( master )

        self.master = master
        self.slave = slave

    def start( self, auto_failover,  call_back):
        if not self.__work_t1 is None:
            raise Exception( "has running" )

        conn_info = self.master.query_connect_info() 
        call_back( ( conn_info[0], conn_info[3] ) )
        self.__work_t1_stop = False
        self.__work_t1 = threading.Thread( target=self.run, args=(self,) ) 
        self.__work_t1.start()

    def stop(self):
        if self.__work_t1 is None:
            return
        logger.debug( "recv stop" )
        self.__work_t1_stop = True 
        self.__work_t1.join()
        self.__work_t1 = None

    ##循环任务
    @staticmethod
    def run(self):

        while not self.__work_t1_stop:
            time.sleep( 3 )
            try:
                if not self.slave.is_my_master( self.master ):
                    logger.info( "old master online and switch to slave" )
                    self.slave.switch_to_slave( self.master )

                self.slave_maintain()

                logger.info( "[{}] [{}]".format( self.master, self.slave ))
                logger.info( "slave 缺少的GTID:[{}] master缺少的GTID:[{}]".format( self.slave.gtid_miss( self.master), self.master.gtid_miss( self.slave ) ) )
            except MySQLConnShutdown as e:
                logger.debug( "检测到过程中连接断开" )
                continue

