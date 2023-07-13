from python:3.8.16-alpine3.17

#RUN pip3 install mysql-connector-python
#RUN --mount allows you to create filesystem mounts that the build can access. This can be used to:
WORKDIR /data
RUN --mount=type=bind,source=./lib,target=/data pip3 install *.whl

#修复了连接报错
COPY ./network.py /usr/local/lib/python3.8/site-packages/mysql/connector/

WORKDIR /root/failover
COPY ./main.py ./
COPY ./mysql_ha/*.py ./mysql_ha/
COPY ./cfg/*.py ./cfg/
COPY ./tcp_proxy/*.py ./tcp_proxy/
CMD [ "/usr/local/bin/python3", "main.py" ]
