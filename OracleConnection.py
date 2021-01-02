import cx_Oracle


def createOracleConnection():
    ip = '<stats4p ip pls>'
    port = 1521
    SID = 'sid'

    dsn_tns = cx_Oracle.makedsn(ip, port, SID)

    try:
        connection = cx_Oracle.connect('username', 'pass', dsn_tnsg)
        print("Connection Created successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)


    return connection

