import dbconnection

# fill data info array by hand or see how to get data from information_schema.tables at 'splitTbale.py'
tidb = []
tbl = []

# eg. SELECT concat(ROUND(SUM(data_length+index_length)/1024/1024/1024,2),'GB')
# FROM information_schema.tables WHERE table_schema='db3' and TABLE_NAME = 'tb1';

for i in range(0, len(tbl)):
    tbl_name = tbl[i]
    db_name = tidb[i]
    sql = "SELECT CONCAT(ROUND(SUM(data_length)/1024/1024/1024,3),'GB') AS data_cap, " \
          "CONCAT(ROUND(SUM(index_length)/1024/1024/1024,3),'GB') AS index_cap " \
          "FROM INFORMATION_SCHEMA.TABLES WHERE table_schema IN (" + "'"+db_name.lower() +"'"+","+"'"+db_name.upper()+"'"+")" + \
          " AND TABLE_NAME IN (" + "'" + tbl_name.lower() + "'" + "," + "'" + tbl_name.upper() + "'" + ")"+";"
    connection = dbconnection.getconnection(db_name)
    cursor = connection.cursor()
    cursor.execute(sql)
    tidb_rows = cursor.fetchall()
    for row in tidb_rows:
        print(db_name+"."+tbl_name, row['data_cap'], row['index_cap'])
