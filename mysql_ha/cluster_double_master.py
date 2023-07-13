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

logger = logging.getLogger()

class MySQlDoubleMasterTopology:
    pass

