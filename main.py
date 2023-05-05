import os
import sys
import threading
import mysql_monitor
from tcp_proxy import tcp_proxy
import logging
import traceback
import time
import logging.handlers


nodeList = os.environ.get( "NODE", "root:000000@mysql-node1:3306,root:000000@mysql-node2:3306" ).split( "," )
log_level = 'DEBUG' if os.environ.get( 'DEBUG', 'false' ).lower() == 'true' else 'INFO'

formatter = logging.Formatter( '%(asctime)s %(threadName)-10s line:%(lineno)-4d %(levelname)7s %(funcName)16s():%(message)s' )

# 创建一个 RotatingFileHandler 实例
rotate_handler = logging.handlers.RotatingFileHandler(filename='/var/log/mysql_failover.log', mode='a', maxBytes=100*1024*1024, backupCount=5)
rotate_handler.setLevel(log_level)
rotate_handler.setFormatter(formatter)

# 配置控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel( log_level )
console_handler.setFormatter(formatter)

# 将处理器添加到日志记录器中
logger = logging.getLogger()
logger.setLevel( log_level )

logger.addHandler(rotate_handler)
logger.addHandler(console_handler)

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


