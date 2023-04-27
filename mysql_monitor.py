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

ER_SLAVE_RLI_INIT_REPOSITORY = "Slave failed to initialize relay log info structure from the repository"

logger = logging.getLogger(__name__)

#DEBUG = os.environ.get( 'DEBUG', 'false' ).lower() == 'true'
#
#logging.basicConfig(level = logging.DEBUG if DEBUG else logging.INFO ,format = '%(asctime)s %(threadName)-10s line:%(lineno)-4d %(levelname)7s %(funcName)16s():%(message)s')
#logger = logging.getLogger(__name__)

df_path="/"

CACHE_SIZE=1024*16
MYSQL_CONN_PENDING   = 0      #接收到客户端的连接请求并就绪后，连接到mysql的请求连接还未就绪,这时只可以从clinet接收数据，但是不可以向数据库发送数据
MYSQL_CONN_READY     = 1      #连接到mysqld的连接已就绪，可以在双方进行数据传输

class NodeOS:
    def disk_is_full():
        try:
            r = shutil.disk_usage( df_path )
            disk_space_free_perc = (r.free / r.total ) * 100
            logger.debug( "current path [%s] disk free usage: [%.2f]" % ( df_path, disk_space_free_perc))
            return disk_space_free_perc > 5
        except FileNotFoundError as e:
            logger.debug( "disk_is_full fails.", e )
            return False

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

#class MySQLSlaveSQLThreadException( mysql.connector.errors.Error):
#    def __init__(self, errno, errmsg):
#        super().__init__( msg=errmsg, errno=errno)

class MySQLGTIDInconsistencyException( mysql.connector.errors.Error):
    def __init__(self, errmsg):
        super().__init__( msg=errmsg )

class MySQLConnShutdown( mysql.connector.errors.Error ):
    pass

class MySQLDb:
    REPL_USER_NAME="repl_scott"
    REPL_USER_PASSWORD="000000"

    def __init__(self, dsn ):
        self.dsn = dsn

        try:
            self.mydb = mysql.connector.connect( ** parse_dsn( dsn ), init_command="set @@sql_log_bin=off" )
        except mysql.connector.errors.InterfaceError as e:
            if e.errno == 2003:
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
            logger.error( e )
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
        rst = self.exec_query( "select SERVICE_STATE, LAST_ERROR_NUMBER, LAST_SEEN_TRANSACTION, LAST_ERROR_MESSAGE, LAST_ERROR_TIMESTAMP from performance_schema.replication_applier_status_by_worker" )

        for item in rst:
            if item[0] == "ON":
                continue 
            if item[1] != 0:
                raise MySQLSlaveSQLThreadException( item[1], item[2], item[3] ) 
            else:
                return False    #错误号为0，应该是执行了 stop slave;

        return True 

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
        self.exec_query( "stop slave; SET GTID_NEXT='{}'; BEGIN; COMMIT; SET GTID_NEXT='AUTOMATIC';start slave;".format( gtid ), multi=True )

class MySQLHATopologyAbstract(ABC):
    @abstractmethod
    def start( self, call_back ):
        pass

    @abstractmethod
    def stop( self ):
        pass
    
class MySQlDoubleMasterTopology:
    pass

class MySQLMasterSlaveCluster(MySQLHATopologyAbstract):
    def __init__( self, master, slave, init):
        if init:
            master.gtid_clean()
            slave.gtid_clean()
            master.switch_to_master()
            slave.switch_to_slave( master )

        self.__work_t1 = None

        slave_miss_gtid = slave.gtid_miss( master)
        master_miss_gtid = master.gtid_miss( slave )

        if len( slave_miss_gtid) > 0:
            logger.warning( "slave缺少GTID:[{}]".format( slave_miss_gtid ) )

        if len( master_miss_gtid ) > 0:
            logger.warning( "master缺少GTID:[{}]".format( master_miss_gtid ) )

        self.master = master
        self.slave = slave

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
            if e.errno == 1007:
                #'Can't create database 't'; database exists' on query
                self.slave.skip_transactions_with_gtid( e.gtid )
            elif e.errno == 1032: 
                #Could not execute Delete_rows event on table t.t; 
                #Can't find record in 't', Error_code: 1032
                self.slave.skip_transactions_with_gtid( e.gtid )
            elif e.errno == 1677:
                #Column 1 of table 'x.t' cannot be converted 
                #from type 'varchar(100(bytes))' to type 'varchar(200(bytes) latin1
                #主从表数据类型不一致
                pass    #目前不支持自动修复
            else:
                raise e

    ##循环任务
    ##等待失效的master重启后，将其角色转变为新的slave
    ##检测当前master是否有效
    ##当master失效而且slave有效时，进行主从切换
    @staticmethod
    def run(self):
        conn_info = self.master.query_connect_info() 
        self.__call_back( ( conn_info[0], conn_info[3] ) )

        while not self.__work_t1_stop:
            time.sleep( 3 )
            try:
                #判断主是否有效
                for i in range( 0,3,1):
                    if self.master.is_connected():
                        logger.debug( "master running" )
                        break 
                    logger.error( "try reconnect master" )
                else:
                    logger.error( "master node connect fails. slave switch_to_master" )
                    self.slave.switch_to_master()
                    self.master, self.slave = self.slave, self.master
                    conn_info = self.master.query_connect_info() 
                    self.__call_back( ( conn_info[0], conn_info[3] ) )
                    continue

                if not self.slave.is_my_master( self.master ):
                    logger.info( "old master online and switch to slave" )
                    self.slave.switch_to_slave( self.master )

                self.slave_maintain()

                logger.info( "[{}] [{}]".format( self.master, self.slave ))
                logger.info( "slave 缺少的GTID:[{}] master缺少的GTID:[{}]".format( self.slave.gtid_miss( self.master), self.master.gtid_miss( self.slave ) ) )
            except MySQLConnShutdown as e:
                logger.debug( "检测到过程中连接断开" )
                continue

    def start( self, call_back):
        if not self.__work_t1 is None:
            raise Exception( "has running" )

        self.__work_t1_stop = False
        self.__call_back = call_back
        self.__work_t1 = threading.Thread( target=self.run, args=(self,) ) 
        self.__work_t1.start()

    def stop(self):
        if self.__work_t1 is None:
            return
        logger.debug( "recv stop" )
        self.__work_t1_stop = True 
        self.__work_t1.join()
        self.__work_t1 = None

#通过数据库的在线状态，扫描集群拓扑结构
#递归扫描，检测结果是一个？
#第一版暂时先支持两个节点, 拓扑是可以通过一个节点，扫描到集群中所有的节点
def scan_cluster_topology( dsn1, dsn2):
    try:
        node1 = MySQLDb( dsn1 )
        node2 = MySQLDb( dsn2 )
    except MySQLConnShutdown:
        return None

    node1_is_master = node1.is_master()
    node1_is_slave = node1.is_slave()

    node2_is_master = node2.is_master()
    node2_is_slave = node2.is_slave()

    logger.debug( "%s %s %s %s", node1_is_master, node1_is_slave, node2_is_master, node2_is_slave )

    if ( node1_is_master and node2_is_master and node1_is_slave and node2_is_slave ) == True:
        if ( node1.is_my_master( node2) and nod2.is_my_master( node1 ) ) == True:
            #互为双主结构
            return MySQlDoubleMasterTopology(node1, node2)

    if ( node1_is_master or node2_is_master or node1_is_slave or node2_is_slave ) == False:
        logger.debug( "create master-slave cluster" )
        return MySQLMasterSlaveCluster( node1, node2, True )

    if node1.is_my_master( node2 ):
        return MySQLMasterSlaveCluster( node2, node1, False )

    if node2.is_my_master( node1 ):
        return MySQLMasterSlaveCluster( node1, node2, False )

