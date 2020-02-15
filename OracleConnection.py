import cx_Oracle


def createOracleConnection():
    ip = '10.133.154.175'
    port = 1521
    SID = 'stat4p'

    dsn_tns = cx_Oracle.makedsn(ip, port, SID)

    try:
        connection = cx_Oracle.connect('GLOBAL_GSM', 'xc%$q2', dsn_tns)
        print("Connection Created successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)


    return connection

