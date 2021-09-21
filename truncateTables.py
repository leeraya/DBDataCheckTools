import dbconnection

# fill data info array by hand or see how to get data from information_schema.tables at 'splitTbale.py'
tidb_name = []
tbl = []
for i in range(0, len(tbl)):
    print(len(tbl), '/', i + 1)
    db_name = tidb_name[i]
    tbl_name = tbl[i]
    tidb_sql = ' truncate table ' + db_name + '.' + tbl_name
    connection = dbconnection.getconnection(db_name)
    cursor = connection.cursor()
    cursor.execute(tidb_sql)
    print('TRANCATE TABLE：', db_name + '.' + tbl_name)
    countSql = 'select count(1) from ' + db_name + '.' + tbl_name
    cursor.execute(countSql)
    tidb_rows = cursor.fetchall()
    for row in tidb_rows:
      cur_count = str(row['count(1)'])
      print('After Trancate Table. Count:', cur_count)
    print('')
    connection.close()