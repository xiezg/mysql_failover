from python:3.8.16-alpine3.17
RUN pip3 install mysql-connector-python
WORKDIR /root
COPY main.py mysql_monitor.py .
COPY tcp_proxy/*.py ./tcp_proxy/
CMD [ "/usr/local/bin/python3", "main.py" ]
