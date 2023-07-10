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

logger = logging.getLogger()

## db://user:password@host:port/database?opt1=val1&opt2=val2
def parse_dsn( dsn ):
    if '@' not in dsn:
        dsn = dsn + "@127.0.0.1:3306"

    part1, part2 = dsn.split( '@' )

    if ':' not in part1:
        part1= part1 + ":"

    if ':' not in part2:
        part2= part2 + ":3306"

    user,passwd = part1.split(':')
    host,port = part2.split(':')

    return { 'user':user, 'passwd':passwd, 'host':host, 'port':int(port)}

class MySQLDb:
    REPL_USER_NAME="repl_scott"
    REPL_USER_PASSWORD="000000"

    def __init__(self, dsn ):
        self.dsn = dsn

        try:
            self.mydb = mysql.connector.connect( ** parse_dsn( dsn ), init_command="set @@sql_log_bin=off" )
        except mysql.connector.errors.InterfaceError as e:
            if e.errno == 2003 or e.errno == -2:
                logger.error( "connect mysql [{}] fails.".format(dsn) )
                raise MySQLConnShutdown()
            raise e

    def __str__(self):
        role = "unknow"
        if self.is_connected():
            if self.is_master():
                role="master"
            elif self.is_slave():
                role= "slave"

        return "dns[{}] role[{}]".format( self.dsn, role )

    def exec_query(self, operation, multi=False):
        if not self.is_connected():
            raise MySQLConnShutdown 

        try:
            mycursor = None
            mycursor = self.mydb.cursor()
            if multi:
                rst=[]
                for result in mycursor.execute(operation, multi=True):
                    logger.debug( "SQL:[{}]".format( result.statement) )
                    if result.with_rows:
                        rst += result.fetchall()
                return rst
            else:
                mycursor.execute( operation, multi=False)
                logger.debug( "SQL:[{}]".format( mycursor.statement) )
                return mycursor.fetchall()
        except mysql.connector.errors.OperationalError as e:
            if e.msg == "MySQL Connection not available":
                raise MySQLConnShutdown
            raise e
        except mysql.connector.errors.InterfaceError as e:
            raise e 
        except mysql.connector.errors.DatabaseError as e:
            raise e
        except mysql.connector.Error as e:
            raise e 
        finally:
            if mycursor:
                mycursor.close()

    def is_connected(self):
        try:
            self.mydb.ping(reconnect=True, attempts=5, delay=2)
        except Exception as e:
            logger.error( str(e) )
            return False  # This method does not raise
        return True

    def query_my_uuid(self):
        return self.query_global_variables( 'server_uuid' )

    def query_my_slave_uuid(self):
        rst = self.exec_query( "show slave hosts;" )
        list=[]
        for item in rst:
            list.append( item[4] )
        return list

    def is_master(self):
        return len( self.exec_query("show slave hosts")) > 0

    def is_slave(self):
        ##select * from replication_connection_configuration
        rst = self.exec_query("select count(*) as n from mysql.slave_master_info;")[0][0]
        if rst > 1:
            raise MySQLMultiSourceUnSupport
        return rst == 1

    def query_my_master_uuid(self):
        ##select * from replication_connection_configuration
        #在启动主从后，假如主从同步发生错误，则该表中Uuid字段为空.
        rst = self.exec_query( "select Uuid from mysql.slave_master_info;")
        list=[]
        for item in rst:
            list.append( item[0] if isinstance( item[0], str) else item[0].decode("utf-8") )
        return list

    def is_my_master(self, db):
        return self.is_slave() and ( db.query_my_uuid() in self.query_my_master_uuid() )

    def is_my_slave(self, db):
        return self.is_master() and ( db.query_my_uuid() in self.query_my_slave_uuid() )

    def create_repl_user(self):
        try:
            self.exec_query( "drop user if exists '%s'@'%%';"%( self.REPL_USER_NAME ) )
            #self.exec_query( "drop user '%s'@'%%';"%( self.REPL_USER_NAME ) )
            self.exec_query( "CREATE USER '%s'@'%%' IDENTIFIED BY '%s';" %( self.REPL_USER_NAME, self.REPL_USER_PASSWORD) )
            self.exec_query( "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%';"%(self.REPL_USER_NAME) )
        except DatabaseError as e:
            raise e

    def query_connect_info( self ):
        connect_info = parse_dsn( self.dsn )
        return ( connect_info['host'], self.REPL_USER_NAME, self.REPL_USER_PASSWORD, connect_info['port'] )

    def is_slave_io_thread_running(self):
        ##select * from replication_connection_status
        #return len( self.exec_query("select * from sys.session where user='sql/slave_io'")) > 0
        rst = self.exec_query( "select SERVICE_STATE,LAST_ERROR_NUMBER,LAST_ERROR_MESSAGE from performance_schema.replication_connection_status" )
        if len( rst) > 1:
            raise Exception( "暂时不支持多通道复制模式" )

        rst = rst[0]
        if rst[0] == "ON":
            return True

        #io thread停止工作，但是错误号为0，可能是主动停止，并不是因为错误停止
        if rst[1] == 0:
            return False

        raise MySQLSlaveIOThreadException( errno=rst[1], errmsg=rst[2] )

    def is_slave_sql_thread_running(self):
        ##return len( self.exec_query("select * from sys.session where user='sql/slave_sql'")) > 0
        rst=True

        rst = self.exec_query( "select SERVICE_STATE, LAST_ERROR_NUMBER, LAST_SEEN_TRANSACTION, LAST_ERROR_MESSAGE, LAST_ERROR_TIMESTAMP from performance_schema.replication_applier_status_by_worker" )
        rst = rst +  self.exec_query( "select SERVICE_STATE, LAST_ERROR_NUMBER, \"\" as LAST_SEEN_TRANSACTION, LAST_ERROR_MESSAGE, LAST_ERROR_TIMESTAMP from performance_schema.replication_applier_status_by_coordinator" )


        for item in rst:
            if item[0] == "ON":
                continue 
            if item[1] != 0:
                raise MySQLSlaveSQLThreadException( item[1], item[2], item[3] ) 
            rst = False    #错误号为0，应该是执行了 stop slave;

        return rst 

    def restart_slave(self):
        if not self.is_slave():
            raise Exception("node not slave")

        try:
            self.exec_query( "start slave" )
        except mysql.connector.errors.DatabaseError as e:
            if e.msg == ER_SLAVE_RLI_INIT_REPOSITORY:
                self.exec_query( "reset slave" )
                self.exec_query( "start slave" )
                return
            raise e

    def switch_to_slave(self, master_node):
        self.exec_query( "stop slave;" )
        self.exec_query( "reset slave all;" )
        self.exec_query( "change master to master_host='%s',master_user='%s', master_password='%s', master_port=%d, MASTER_AUTO_POSITION=1" % ( master_node.query_connect_info()  ) )
        self.exec_query( "set global super_read_only=on;" )
        self.exec_query( "start slave;" )

    def switch_to_master(self):
        ##暂时未考虑slave延迟的问题。及sql线程仍在执行中继日志中的任务
        ##是否需要等待，等待多长时间，等待的期间，数据库是不可用的。
        self.exec_query( "stop slave;" )
        self.exec_query( "reset slave all;" )
        self.exec_query( "set global super_read_only=off;" )
        self.create_repl_user()

    def query_global_variables(self, name):
        return self.exec_query( "select VARIABLE_VALUE from performance_schema.global_variables where VARIABLE_NAME like '{}'".format( name) )[0][0]

    def gtid_clean(self):
        self.exec_query( "reset master" )

    def gtid_append( self, sub_gtid ):
        current_gtid = self.query_global_variables( "gtid_executed" )
        current_gtid += ","
        current_gtid += sub_gtid 
        logger.debug( current_gtid )
        self.gtid_clean()
        self.exec_query( "set global gtid_purged='{}'".format( current_gtid ) )

    def gtid_miss( self, target):
        target_gtid = target.query_global_variables( "gtid_executed" )
        my_gtid = self.query_global_variables( "gtid_executed" )
        return self.exec_query( "SELECT GTID_SUBTRACT('{}','{}') as n".format( target_gtid, my_gtid) )[0][0]

    def gtid_force_sync(self, target):
        self.gtid_append( self.gtid_miss(target) )

    def skip_transactions_with_gtid( self, gtid ):
        if len( str(gtid) ) <= 0:
            raise Exception( "gtid:[{}] is empty".format( gtid ) )

        self.exec_query( "stop slave; SET GTID_NEXT='{}'; BEGIN; COMMIT; SET GTID_NEXT='AUTOMATIC';start slave;".format( gtid ), multi=True )

