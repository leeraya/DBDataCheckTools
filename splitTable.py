import dbconnection

# Split table before batch insert into table. It can speed up the insertion process

# such as t1.tbl1, t1.tbl2, t2.tbl1,t2.tbl3
# [tidb] : [t1, t1, t2, t2]
# [tbl]  : [tb1, tbl2, tbl1, tbl3]

# keep target tidb database names in it
tidb = ['db1', 'db2', 'db2', 'db2', 'db3']

# keep target tidb table names in it
tbl = ['tbl1', 'tbl1', 'tbl2', 'tbl3', 'tbl1']

# Also， Querying the system table `INFORMATION_SCHEMA.Tables` can also get the db and table information.
# eg. select table_schema, table_name from INFORMATION_SCHEMA.TABLES where table_schema
# not in ('mysql','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','test','METRICS_SCHEMA','xxljob_db')
# 'not in (xxxx)' can exclude unrelated db and table
# keep target tidb database names in it
tidb = []
# keep target tidb table names in it
tbl = []
systable = "INFORMATION_SCHEMA"
query_sql = "select table_schema, table_name from INFORMATION_SCHEMA.TABLES where table_schema not in ('mysql','INFORMATION_SCHEMA','PERFORMANCE_SCHEMA','test','METRICS_SCHEMA')"
connection = dbconnection.getconnection(systable)
cursor = connection.cursor()
cursor.execute(query_sql)
tidb_rows = cursor.fetchall()
for row in tidb_rows:
    tidb.append(str(row['table_schema']).upper())
    tbl.append(str(row['table_name']).upper())


# Choose one of the ways to fill data into tidb[] and tbl[] above.

# eg. SPLIT TABLE TEST_HOTSPOT BETWEEN (0) AND (9223372036854775807) REGIONS 128;
region_num = 128;
for i in range(0, len(tbl)):
    tbl_name = tbl[i]
    db_name = tidb[i]
    sql = " SPLIT TABLE " + db_name + "." + tbl_name + " BETWEEN (0) AND (9223372036854775807) REGIONS " + str(region_num) + ";"
    connection = dbconnection.getconnection(db_name)
    cursor = connection.cursor()
    cursor.execute(sql)
    # query split result
    show_sql = " SHOW TABLE " + db_name+"."+tbl_name+" REGIONS; "
    cursor.execute(show_sql)
    region_count = int(cursor.rowcount)
    if region_count < region_num:
        print("Failed split Region !!!")
connection.commit()