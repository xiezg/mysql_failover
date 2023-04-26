#########################################################################
# File Name: build.sh
# Author: xiezg
# mail: xzghyd2008@hotmail.com
# Created Time: 2023-03-14 19:58:23
# Last modified: 2023-04-03 18:37:38
#########################################################################
#!/bin/bash

####scp root@bj-cpu065.aibee.cn:/root/xiezg_work/mysql_proxy/mysql_proxy.py ./
#####scp root@bj-cpu065.aibee.cn:/root/xiezg_work/mysql_proxy/deploy.yaml ./

image_name=harbor.aibee.cn/platform/mysql-failover:v0.0.1

docker build -t $image_name .
docker push $image_name

