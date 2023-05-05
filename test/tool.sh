#########################################################################
# File Name: tool.sh
# Author: xiezg
# mail: xzghyd2008@hotmail.com
# Created Time: 2023-03-29 19:21:25
# Last modified: 2023-05-05 15:49:33
#########################################################################
#!/bin/bash

pushd `dirname $0`

cat >.env <<EOF
BASE_PATH=`pwd`
IMAGE_HOST=harbor.aibee.cn
EOF

mkdir -p ./dir1
mkdir -p ./dir2
mkdir -p ./backup

wait_ready(){
    docker ps | grep mysql

    until docker exec -it mysql-node1 mysqladmin ping -uroot -p000000 2>/dev/null 1>&2
    do
        sleep 1
    done

    until docker exec -it mysql-node2 mysqladmin ping -uroot -p000000 2>/dev/null 1>&2
    do
        sleep 1
    done
}

insert_into (){

docker exec -i $1 mysql -uroot -p000000 << EOF 
create database if not exists t; 
create table if not exists t.t (id int, msg char(10));
EOF

for(( i=0;i<50;i++))
do
    echo $i
    docker exec -it $1 mysql -uroot -p000000 -e "insert into t.t (id, msg) values($i, 'hehe');"
    docker exec -it $1 mysql -uroot -p000000 -e "show master status;"
done
}


slave_1007(){

docker exec -i mysql-node2 mysql -uroot -p000000 << EOF 
set global super_read_only=off;
SET sql_log_bin = OFF;
drop database t;
create database t;
start slave;
EOF

docker exec -i mysql-node1 mysql -uroot -p000000 << EOF 
SET sql_log_bin = OFF;
drop database t;
SET sql_log_bin = ON;
create database t;
EOF

docker exec -i mysql-node2 mysql -uroot -p000000 << EOF 
show slave status\G
EOF
}

###模拟数据库发生1236错误
slave_1236(){

    docker exec -it mysql-node2 mysqladmin -uroot -p000000 stop-slave;
    insert_into mysql-node1
    docker exec -it mysql-node1 mysql -uroot -p000000 -e "FLUSH BINARY LOGS;";
    docker exec -it mysql-node1 mysql -uroot -p000000 -e "purge BINARY LOGS BEFORE '2024-04-05 00:00:00'";
    docker exec -it mysql-node1 mysql -uroot -p000000 -e "show master status";
    docker exec -it mysql-node2 mysqladmin -uroot -p000000 start-slave;
    docker exec -it mysql-node2 mysql -uroot -p000000 -e "show slave status\G";
}

slave_1008(){
docker exec -i mysql-node1 mysql -uroot -p000000 << EOF 
SET sql_log_bin = OFF;
create database if not exists t11;
SET sql_log_bin = ON;
drop database t11;
EOF
}

slave_1146(){
docker exec -i mysql-node1 mysql -uroot -p000000 << EOF 
SET sql_log_bin = OFF;
create database if not exists t13;
use t13;
create table t ( id int );
insert into t ( id ) values( 10 );
SET sql_log_bin = ON;
insert into t ( id ) values( 12 );
EOF
}

slave_1051(){
docker exec -i mysql-node1 mysql -uroot -p000000 << EOF 
SET sql_log_bin = OFF;
create database if not exists t12;
use t12;
create table t(id int);
SET sql_log_bin = ON;
drop table t;
EOF
}

case $1 in
test_1236)
    insert_into mysql-node1
    ;;
1007)
    slave_1007
    ;;
1008)
    slave_1008
    ;;
1146)
    slave_1146
    ;;
1051)
    slave_1051
    ;;
1236)
    slave_1236
    ;;
reset)
    docker-compose -f docker-compose.yaml down
    rm -rf ./dir1/*
    rm -rf ./dir2/*
    docker-compose -f docker-compose.yaml up -d
    wait_ready
    ;;
stop)
    docker-compose -f docker-compose.yaml down
    ;;
start)
    docker-compose -f docker-compose.yaml up -d
    wait_ready
    ;;
restart)
    docker-compose -f docker-compose.yaml down
    docker-compose -f docker-compose.yaml up -d
    wait_ready
    ;;
*)
    exit 1;;
esac

popd
