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

ER_SLAVE_RLI_INIT_REPOSITORY = "Slave failed to initialize relay log info structure from the repository"

logger = logging.getLogger()

class MySQLMasterSlaveCluster(MySQLSingleMasterSingleSlaveCluster):
    def __init__( self, master, slave, init, auto_failover=None ):
        if init:
            master.gtid_clean()
            slave.gtid_clean()
            master.switch_to_master()
            slave.switch_to_slave( master )
            #for i in range( 10 ):
            while True:
                if len( slave.query_my_master_uuid() ) > 0:
                    break;
                time.sleep(1)
            else:
                raise Exception( "slave can't find master uuid" )

        self.__work_t1 = None

        slave_miss_gtid = slave.gtid_miss( master)
        master_miss_gtid = master.gtid_miss( slave )

        if len( slave_miss_gtid) > 0:
            logger.warning( "slave缺少GTID:[{}]".format( slave_miss_gtid ) )

        if len( master_miss_gtid ) > 0:
            logger.warning( "master缺少GTID:[{}]".format( master_miss_gtid ) )

        self.master = master
        self.slave = slave

    def master_is_alive(self):
        try:
            for i in range(3):
                if self.master.is_connected( timeout=5 ):
                    return True
            else:
                return False
        except:
            return False

    ##循环任务
    ##等待失效的master重启后，将其角色转变为新的slave
    ##检测当前master是否有效
    ##当master失效而且slave有效时，进行主从切换
    @staticmethod
    def failover(self, interval):
        conn_info = self.master.query_connect_info() 
        self.__call_back( ( conn_info[0], conn_info[3] ) )

        while not self.__work_t1_stop:
            time.sleep( interval )

            if not self.master_is_alive():
                #master节点失效
                logger.warning( "master not alive" )
                if not self.__auto_failover:
                    #禁止故障转移，继续探测
                    continue

                if not self.slave.is_connected( 3 ):
                    #slave节点失效，继续探测
                    continue

                try:
                    #进行主从节点切换
                    self.slave.switch_to_master()
                    self.master, self.slave = self.slave, self.master
                except:
                    #主从切换失败
                    logger.error( "master failover fails. [{}]".format( sys.exception() ) )
                    continue
                else:
                    conn_info = self.master.query_connect_info() 
                    self.__call_back( ( conn_info[0], conn_info[3] ) )
                    continue 

            try:
                if not self.slave.is_my_master( self.master ):
                    logger.info( "old master online and switch to slave" )
                    self.slave.switch_to_slave( self.master )
            except:
                logger.error( "slave online fails. [{}]".format( sys.exception() ) )
                continue

            if False:
                try:
                    self.slave_maintain()
                    logger.info( "[{}] [{}]".format( self.master, self.slave ))
                    logger.info( "slave 缺少的GTID:[{}] master缺少的GTID:[{}]".format( self.slave.gtid_miss( self.master), self.master.gtid_miss( self.slave ) ) )
                except:
                    pass

    #启动一主一从高可用拓扑监控
    def start_failover( self, interval, auto_failover,  call_back):
        if not self.__work_t1 is None:
            raise Exception( "has running" )

        self.__auto_failover = auto_failover
        self.__work_t1_stop = False
        self.__call_back = call_back
        self.__work_t1 = threading.Thread( target=self.failover, args=(self,interval) ) 
        self.__work_t1.start()

    def stop_failover(self):
        if self.__work_t1 is None:
            return
        logger.debug( "recv stop" )
        self.__work_t1_stop = True 
        self.__work_t1.join()
        self.__work_t1 = None

