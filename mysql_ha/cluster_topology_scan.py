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

from mysql_ha.cluster_master_slave      import MySQLMasterSlaveCluster
from mysql_ha.cluster_double_master     import MySQlDoubleMasterTopology
from mysql_ha.cluster_fix_master_slave  import MySQLFixMasterSlaveCluster

logger = logging.getLogger()

#通过数据库的在线状态，扫描集群拓扑结构
#递归扫描，检测结果是一个？
#第一版暂时先支持两个节点, 拓扑是可以通过一个节点，扫描到集群中所有的节点
def scan_cluster_topology( dsn1, dsn2, master = None ):
    try:
        node1 = MySQLDb( dsn1 )
        node2 = MySQLDb( dsn2 )
    except MySQLConnShutdown:
        return None

    node1_is_master = node1.is_master()
    node1_is_slave = node1.is_slave()

    node2_is_master = node2.is_master()
    node2_is_slave = node2.is_slave()

    logger.debug( "node1[%s]:(isMaster:%s isSlave:%s)", dsn1, node1_is_master, node1_is_slave )
    logger.debug( "node2[%s]:(isMaster:%s isSlave:%s)", dsn2, node2_is_master, node2_is_slave )

    #空节点表示（没有建立主从结构的节点，但是不代表没有数据）
    empty_node = ( node1_is_master or node2_is_master or node1_is_slave or node2_is_slave ) == False

    #建立固定的主从关系
    if master:
        master_node = node1 if master == dsn1 else node2
        slave_node = node2 if master == dsn1 else node1
        return MySQLFixMasterSlaveCluster ( master_node, slave_node, empty_node )

    #已存在的双主关系
    if ( node1_is_master and node2_is_master and node1_is_slave and node2_is_slave ) == True:
        if ( node1.is_my_master( node2) and nod2.is_my_master( node1 ) ) == True:
            #互为双主结构
            return MySQlDoubleMasterTopology(node1, node2)

    #节点主从角色为空(不代表数据为空)，则重新建立
    if empty_node:
        logger.debug( "create master-slave cluster" )
        return MySQLMasterSlaveCluster( node1, node2, True )

    #已存在的主从关系
    if node1.is_my_master( node2 ):
        return MySQLMasterSlaveCluster( node2, node1, False )

    #已存在的主从关系
    if node2.is_my_master( node1 ):
        return MySQLMasterSlaveCluster( node1, node2, False )

    raise Exception ( "can't create mysqlha topology" )
