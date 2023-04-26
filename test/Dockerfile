from python:3.8.16-alpine3.17
RUN pip3 install mysql-connector-python
COPY ./mysql_proxy.py /mysql_proxy.py
COPY ./*.py /
CMD python3 /main.py
