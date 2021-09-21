import dbconnection
import cx_Oracle

## The script is used to check if data from oracle to tidb is consistent.
## the migration is processed by DSG(http://www.dsgdata.com/).
## The config table 'DSG_SYNC_xxx' is created to map table between ORACLE and TiDB

## for simple situation, just fill db and table info into the array is also ok.
tidb = []
ora_db = []
tbl = []

# get oracle connection
dns_tns = cx_Oracle.makedsn('host', 'port', service_name='orcl')
ora_con = cx_Oracle.connect('username', 'pwd', dns_tns)
ora_cur = ora_con.cursor()

# sync_table is a self config table to describe mapping configuration.
# The table is like this:
# u | t
# 'DB_NAME1' | 'TABLE_NAME1'
# 'DB_NAME1' | 'TABLE_NAME3'
# 'DB_NAME2' | 'TABLE_NAME1'

sync_table = "DSG_SYNC_xxx"
dssql = " SELECT u,t FROM REALSYNC." + sync_table

ora_cur.execute(dssql)
ora_rows = ora_cur.fetchall()

# fill data info array
for row in ora_rows:
  rowStr = str(row)
  rowArr = rowStr.split(", ")
  ora_db_name = rowArr[0].strip("(").strip('\'')
  # if target database names is the same as oracle , just use: tidb_name = ora_db_name.
  # if any differences exists between oracle and tidb, it should be updated according to actual difference !
  tidb_name = ora_db_name[4:]
  tbl_name = rowArr[1].strip(")").strip('\'')
  tbl.append(tbl_name)
  tidb.append(tidb_name)
  ora_db.append(ora_db_name)

# check tidb db and table is empty
# for i in range(0, len(tidb)):
#   db_name = tidb[i]
#   tbl_name = tbl[i]
#   sql = "SELECT count(1) as count from " + db_name+"."+tbl_name
#   conn = dbconnection.getconnection(tidb_name)
#   cursor = conn.cursor()
#   cursor.execute(sql)
#   tidb_rows = cursor.fetchall()
#   for row in tidb_rows:
#     count = str(row['count'])
#     if count != '0':
#       print(db_name+tbl_name,'table count not equal 0 !')
#   conn.close()

# check column
for i in range(0, len(tbl)):
  tidb_name = tidb[i]
  ora_db_name = ora_db[i]
  tbl_name = tbl[i]
  tidb_fields = []
  ora_fields = []
  tidb_sql = " DESC " + tidb_name+"."+tbl_name+" ;"
  # select count(1) from all_tab_columns where Table_Name='EMP_BAS_INFO_B';
  ora_sql = " select COLUMN_NAME from all_tab_columns where Table_Name = '" + tbl_name+"'"

  conn = dbconnection.getconnection(tidb_name)
  cursor = conn.cursor()
  cursor.execute(tidb_sql)
  tidb_rows = cursor.fetchall()
  for row in tidb_rows:
    tidb_fields.append(row['Field'])

  ora_cur.execute(ora_sql)
  ora_rows = ora_cur.fetchall()
  for row in ora_rows:
    rowStr = str(row).strip("(").strip(")").strip('\'').strip('\',')
    ora_fields.append(rowStr)

  if len(tidb_fields) != len(ora_fields):
    print(tbl_name, "column information can not match !!!!!!")

  for i in range(0, len(tidb_fields)):
    tidb_field = str(tidb_fields[i])
    if tidb_field.upper() not in ora_fields and tidb_field.lower() not in ora_fields:
      print(tbl_name, tidb_field , "not in oracle columns!")

  for i in range(0, len(ora_fields)):
    ora_field = str(ora_fields[i])
    if ora_field.upper() not in ora_fields and ora_field.lower() not in ora_fields:
      print(tbl_name, ora_field , "not in tidb columns!")

conn.close()

# used by final result report
ora_rs = []
tidb_rs = []

for i in range(0, len(ora_db)):
  tidb_name = tidb[i]
  ora_db_name = ora_db[i]
  tbl_name = tbl[i]
  
  tidb_sql = 'select count(1) from ' + tidb_name+'.'+tbl_name
  ora_sql = 'select /*+ parallel(t'+',16)*/ count(1) from ' + ora_db_name + '.' + tbl_name + ' t'
  
  connection = dbconnection.getconnection(tidb_name)
  cursor = connection.cursor()

  cursor.execute(tidb_sql)
  tidb_rows= cursor.fetchall()

  ora_cur.execute(ora_sql)

  ora_rows = ora_cur.fetchall()
  for row in  tidb_rows:
       tidb_rs.append(str(row['count(1)']))
  for row in ora_rows:
      ora_rs.append(str(row)[1:-2])
  print(len(tbl), '/', i+1)
  print('对比结果：', tidb_rs[i] == ora_rs[i])
  print(ora_db_name+'.'+tbl_name + ' VS ' + tidb_name + '.' + tbl_name)
  print(ora_rs[i] + " : " + tidb_rs[i])
  print('')
  
print('Count does not match:')
for i in range(0, len(tidb_rs)):
  if ora_rs[i] != tidb_rs[i]:
    print(ora_db[i] + '.' + tbl[i] + ' VS ' + tidb[i] + '.' + tbl[i])
    print(ora_rs[i]+' : '+tidb_rs[i])
    print('')

ora_cur.close()
ora_con.close()
connection.close()