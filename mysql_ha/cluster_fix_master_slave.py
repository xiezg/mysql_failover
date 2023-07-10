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
from mysql_ha.cluster_abs import MySQLSingleMasterSingleSlaveCluster
from mysql_ha.errors import MySQLConnShutdown
from mysql_ha.mysql_db import MySQLDb

ER_SLAVE_RLI_INIT_REPOSITORY = "Slave failed to initialize relay log info structure from the repository"

logger = logging.getLogger()

#固定主节点的主从数据库集群
class MySQLFixMasterSlaveCluster(MySQLSingleMasterSingleSlaveCluster):
    def __init__( self, master, slave, init ):
        if init :
            self. master.gtid_clean()
            slave.gtid_clean()

        #强制切换节点角色
        master.switch_to_master()
        slave.switch_to_slave( master )

        self.master = master
        self.slave = slave

    def start( self, auto_failover,  call_back):
        conn_info = self.master.query_connect_info() 
        call_back( ( conn_info[0], conn_info[3] ) )

    def stop(self):
        pass

