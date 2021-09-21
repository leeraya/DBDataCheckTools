import pymysql.cursors


def getconnection(db_name):
    connection = pymysql.connect(host='127.0.0.1', user='root', port=4000, password='', db=db_name,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
    return connection