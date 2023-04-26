import mysql.connector
import threading
import time

def mysql_ping():
    mydb = mysql.connector.connect( host="127.0.0.1", port=3306, user="root", passwd="000000")

    while True:
        try:
            mydb.ping()
            print( "ping ok" )
        except Exception as e:
            print( e )
            try:
                mydb.reconnect()
                print( "reconnect ok" )
            except mysql.connector.errors.InterfaceError as e:
                pass
        time.sleep( 2 )

for i in range(8):
    t = threading.Thread( target=mysql_ping )
    t.start()

