import configparser
import re

class MyCfg():
    def __init__(self, fpath ):
        # 创建一个ConfigParser对象
        self.config = configparser.ConfigParser( allow_no_value=True )

        # 读取INI文件
        self.config.read( fpath )
        
        # 获取指定section中的所有选项
        #options = config.options('log')
        #print(options)
        
        # 获取指定section中的所有键值对
        #items = config.items('failover')
        #print(items)
        
        # 获取指定section中的某个选项的值
        #value = config.get('section_name', 'option_name')
        #print(value)
        
        # 判断指定section和选项是否存在
        #section_exist = config.has_section('section_name')
        #option_exist = config.has_option('section_name', 'option_name')
        
        # 获取所有的section
        #sections = config.sections()
        #print(sections)

    #从配置文件中获取被监控MySQL节点的DSN，返回一个数组
    #[failover]
    #node1=root:000000@mysql-node1:3306
    #node2=root:000000@mysql-node2:3306
    #...
    def getNodeDSN(self):
        rst=[]
        for item in self.config.items( "failover" ):
            if re.match( "^node[0-9]{1,}$", item[0] ):
                rst.append( item[1] )
        return list(set(rst))  #将rst中的DSN去重

    def getLogLevel(self):
        return self.config.get( "log", "level" )

    def getLogSavePath(self):
        return self.config.get( "log", "save_path" )

    def getProxyListenPort(self):
        return self.config.getint( "proxy", "listen_port" )

    def getProxyWorkThreadNum(self):
        return self.config.getint( "proxy", "work_thread_num" )

    def getProxyTcpIdleTimeout(self):
        return self.config.getint( "proxy", "tcp_idle_timeout" )

    def isEnableFailover(self):
        return self.config.get( "failover", "auto_failover" ).lower() == "true"

    def getFixMasterNode(self):
        if self.config.has_option( "failover", "fix_master" ):
            return self.config.get( "failover", self.config.get( "failover", "fix_master" ) )
        return None

