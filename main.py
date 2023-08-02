import os
import sys
import threading
from mysql_ha import scan_cluster_topology
from tcp_proxy import tcp_proxy
import logging
import traceback
import time
from logging.handlers import RotatingFileHandler
from cfg import cfg

mycfg = cfg.MyCfg( "my.ini" )

log_level = mycfg.getLogLevel()

formatter = logging.Formatter( '%(asctime)s %(threadName)-10s line:%(lineno)-4d %(levelname)7s %(funcName)16s():%(message)s' )
#formatter = logging.Formatter( '%(funcName)16s():%(message)s' )

# 创建一个 RotatingFileHandler 实例
rotate_handler = RotatingFileHandler(filename= mycfg.getLogSavePath(), mode='a', maxBytes=100*1024*1024, backupCount=5)
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
    logger.critical( f'Unhandled exception:[ {args.exc_type} / {args.exc_value}]')
    traceback.print_tb(args.exc_traceback)
    logging.shutdown()
    os._exit(1)

# 设置全局未处理异常处理程序
threading.excepthook =  handle_exception

#没有在代理中直接指定一个主节点，
#是因为主节点必须是通过选主策略获取的
#减少TCPProxy模块和选主模块的耦合
proxy = tcp_proxy.TCPProxy( mycfg.getProxyTcpIdleTimeout() )

def mysql_failover_callback( master_host ):
    proxy.stop()
    proxy.start( mycfg.getProxyListenPort(), master_host, mycfg.getProxyWorkThreadNum() )

nodeList = mycfg.getNodeDSN()
cluster = scan_cluster_topology( nodeList[0], nodeList[1], mycfg.getFixMasterNode() )
if not cluster:
    exit(1)

cluster.start_failover( mycfg.failover_timeout, mycfg.is_enable_failover, mysql_failover_callback )



