# -*- coding: utf-8 -*-
import psycopg2
import pyodbc
import time
import logging

# Connection Information
LOG_LEVEL = logging.ERROR
LOG_FILE_NAME = '{}.log'.format(time.strftime("%Y%m%d"))
ACCDB_CONN_INFO = r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=C:\\_Dev\\PoiToPg\\poi.accdb;"

PG_CONN_INFO = r"dbname='ngii' user='postgres' host='localhost' password='postgres'"
ERR_SQL_FILENAME = "ERR_SQL_{}.sql".format(time.strftime("%Y%m%d"))

# Create Error SQL Collect file
logging.basicConfig(filename=LOG_FILE_NAME, level=LOG_LEVEL, format='%(asctime)-15s %(message)s')
open(ERR_SQL_FILENAME, 'w').close()


# Make safe String for PostgreSQL
def postgres_escape_string(s):
    if not isinstance(s, basestring):
        raise TypeError("%r must be a str or unicode" % (s, ))
    escaped = repr(s)
    if isinstance(s, unicode):
        # assert escaped[:1] == 'u'
        # escaped = escaped[1:]
        escaped = repr(s.encode("UTF-8")) # Collect UTF-8 problum
    if escaped[:1] == '"':
        escaped = escaped.replace("'", "\\'")
    elif escaped[:1] != "'":
        raise AssertionError("unexpected repr: %s", escaped)
    return "E'%s'" % (escaped[1:-1], )

# connect to Access file
ac_conn = None
try:
    pyodbc.lowercase = True

    ac_conn = pyodbc.connect(ACCDB_CONN_INFO)
except Exception as e:
    logging.error("[ERROR] Can't connect to Access file: {}".format(ACCDB_CONN_INFO))
    logging.error(e.args[1])
    exit(-1)

# connect to PostgreSQL
pg_conn = None
try:
    pg_conn = psycopg2.connect(PG_CONN_INFO)
except Exception as e:
    logging.error("[ERROR] Can't connect to PostgreSQL: {}".format(PG_CONN_INFO))
    logging.error(e.message)
    exit(-1)

# Collect table names
tables = None
tableNames = None
retryCnt = 0
while tables is None:
    try:
        ac_cur = ac_conn.cursor()
        tables = ac_cur.tables(tableType="table")
        tableNames = [tbl.table_name for tbl in tables]
        logging.debug(tableNames)
        ac_cur.close()
    except Exception as e:
        retryCnt += 1
        if retryCnt > 10:
            logging.error("[ERROR] Retry time out.")
            exit(-1)
        time.sleep(1)

# === Make DDL ===
for tableName in tableNames:
    sqlArray = list([u"CREATE TABLE"])
    sqlArray.append(u'"{}" ('.format(tableName).lower())

    ac_cur = None
    colSqls = None
    try:
        ac_cur = ac_conn.cursor()
        columns = ac_cur.columns(table=tableName)
        logging.debug(columns.description)
        colSqls = []
        for col in columns:
            colName = col.column_name
            dataType = col.type_name
            if dataType == "COUNTER":
                dataType = "integer"
            if dataType == "INTEGER":
                dataType = "integer"
            if dataType == "DOUBLE":
                dataType = "double precision"
            colSize = col.column_size
            if dataType != "double precision" and dataType != "integer":
                colInfo = u"{} {}({})".format(colName.lower(), dataType, colSize)
            else:
                colInfo = u"{} {}".format(colName.lower(), dataType)
            colSqls.append(colInfo)
    except Exception as e:
        logging.error(e.args[1])

    ac_cur.close()
    sqlArray.append(u", ".join(colSqls))
    sqlArray.append(u")")

    fullSQL = " ".join(sqlArray)

    # Check if PG table exist
    pg_cur = pg_conn.cursor()
    try:
        pg_cur.execute(u"select tablename from pg_tables where schemaname = 'public' and tablename = '{}'"
                       .format(tableName.lower()))
        result = pg_cur.fetchall()
        if len(result) > 0:
            pg_cur.execute(u"DROP TABLE {}".format(tableName.lower()))
        # Make Table
        logging.info(fullSQL)
        pg_cur.execute(fullSQL)
        pg_conn.commit()
    except Exception as e:
        logging.error(u"[SQL ERROR] {}".format(e.message))
    pg_cur.close()

# === Insert data ===
for tableName in tableNames:
    logging.info(u"### Processing table {}".format(tableName))
    print(u"### Processing table {}".format(tableName))

    ac_cur = ac_conn.cursor()
    pg_cur = pg_conn.cursor()

    ac_sql = u"SELECT * FROM {}".format(tableName)
    ac_cur.execute(ac_sql)

    cnt = 0
    err_cnt = 0
    while True:
        result = ac_cur.fetchone()
        if result is None:
            break

        cnt += 1
        if cnt % 10000 == 0:
            print u"{}...".format(cnt),
        if cnt % 100000 == 0:
            print

        # print result.cursor_description
        val_arr = []
        for col_val in result:
            if col_val is None:
                val_arr.append(u"NULL")
            else:
                val_arr.append(postgres_escape_string(unicode(col_val)))

        val_str = ", ".join(val_arr)
        sql = u"INSERT INTO {} VALUES ({})".format(tableName.lower(), val_str)
        # print sql
        try:
            pg_cur.execute(sql)
            pg_conn.commit()
        except Exception as e:
            err_cnt += 1
            pg_conn.commit()
            logging.info(sql)
            with open(ERR_SQL_FILENAME, 'a') as f:
                f.write(u"{};\n".format(sql))
            logging.error(e.message)
        # if cnt >= 10000: break

    logging.info(u"[Table {}] Total row:{}, Error row:{}".format(tableName, cnt, err_cnt))
    print(u"[Table {}] Total row:{}, Error row:{}".format(tableName, cnt, err_cnt))

    ac_cur.close()
    pg_cur.close()

ac_conn.close()
pg_conn.close()
