import os
import sys
import threading
import mysql_monitor
from tcp_proxy import tcp_proxy
import logging
import traceback
import time

nodeList = os.environ.get( "NODE", "root:000000@mysql-node1:3306,root:000000@mysql-node2:3306" ).split( "," )
DEBUG = os.environ.get( 'DEBUG', 'false' ).lower() == 'true'
logging.basicConfig(level = logging.DEBUG if DEBUG else logging.INFO ,format = '%(asctime)s %(threadName)-10s line:%(lineno)-4d %(levelname)7s %(funcName)16s():%(message)s')
logger = logging.getLogger(__name__)

def handle_exception(args):
    # 打印异常类型、值和堆栈跟踪信息
    logger.critical( f'Unhandled exception: {args.exc_value}')
    traceback.print_tb(args.exc_traceback)
    logging.shutdown()
    os._exit(1)

# 设置全局未处理异常处理程序
threading.excepthook =  handle_exception

proxy = tcp_proxy.TCPProxy()

def mysql_failover_callback( master_host ):
    proxy.stop()
    proxy.start( 3306, master_host, 16  )

cluster = mysql_monitor.scan_cluster_topology( nodeList[0], nodeList[1] )
if not cluster:
    exit(1)
cluster.start( mysql_failover_callback )


