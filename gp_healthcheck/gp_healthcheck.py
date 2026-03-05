#!/usr/bin/env python
from __future__ import print_function
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from multiprocessing import Process, Value


class _CmdResult(object):
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


cmd_name = os.path.basename(sys.argv[0])
hostname = "localhost"
port = "5432"
database = ""
username = "gpadmin"
password = "gpadmin"
IS_ALLDB = False
IS_ALLSCHEMA = 0
IS_SKIPUDF = False
FUNC_DIR = ""
CHK_SCHEMA = []
SCHEMA_FILE = ""
concurrency = 2
LOG_DIR = ""
fh_log = None
schema_list = []
schema_str = ""
gpver = ""
dbname_list = []


def get_current_date():
    return datetime.now().strftime("%Y%m%d")


def show_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_log():
    global fh_log
    logday = get_current_date()
    if LOG_DIR == "~/gpAdminLogs":
        logpath = "{0}/gpAdminLogs/{1}_{2}.log".format(os.environ['HOME'], cmd_name, logday)
    else:
        logpath = "{0}/{1}_{2}.log".format(LOG_DIR, cmd_name, logday)
    try:
        fh_log = open(logpath, "a")
    except OSError:
        print("[ERROR]:Cound not open logfile {0}".format(logpath))
        sys.exit(-1)


def info(printmsg):
    if fh_log:
        fh_log.write("[{0} INFO] {1}".format(show_time(), printmsg))
    return 0


def info_notimestr(printmsg):
    if fh_log:
        fh_log.write(printmsg)
    return 0


def error(printmsg):
    if fh_log:
        fh_log.write("[{0} ERROR] {1}".format(show_time(), printmsg))
    return 0


def close_log():
    if fh_log:
        fh_log.close()


def set_env():
    os.environ["PGHOST"] = hostname
    os.environ["PGPORT"] = port
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password


def run_psql(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return proc.returncode, out


def run_psql_simple(sql, extra_flags="-A -X -t"):
    flags = extra_flags.split()
    args = ["psql"] + flags + ["-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return proc.returncode, out


def get_gpver():
    sql = "select version();"
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-d", "postgres"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("Get GP version error!")
        sys.exit(1)
    sver = out.strip()

    if "Greenplum Database" in sver:
        tmpstr = sver.split(" ")
        print(tmpstr[4])
        info_notimestr("GP Version: {0}\n".format(tmpstr[4]))
        tmpver = tmpstr[4].split(".")
        sresult = "gp{0}".format(tmpver[0])
        print(sresult)
    elif "Cloudberry Database" in sver or "Apache Cloudberry" in sver:
        tmpstr = sver.split(" ")
        print(tmpstr[4])
        info_notimestr("CBDB Version: {0}\n".format(tmpstr[4]))
        sresult = "cbdb{0}".format(tmpstr[4])
        print(sresult)
    else:
        sresult = ""

    return sresult


def get_dbname():
    global dbname_list, IS_ALLSCHEMA

    if IS_ALLDB:
        sql = "select datname from pg_database where datname not in ('postgres','template1','template0','gpperfmon','diskquota') order by 1;"
        ret, output = run_psql([
            "psql", "-A", "-X", "-t", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
        ])
        if ret:
            error("Query all database name error\n")
            sys.exit(1)
        dbname_list = [s.strip() for s in output.strip().split("\n") if s.strip()]
        IS_ALLSCHEMA = 1
    else:
        dbname_list = [database]


def get_schema():
    global schema_list, schema_str, IS_ALLSCHEMA

    if CHK_SCHEMA:
        schema_list = list(CHK_SCHEMA)
    elif SCHEMA_FILE:
        if not os.path.exists(SCHEMA_FILE):
            error("Schema file {0} do not exist!\n".format(SCHEMA_FILE))
            sys.exit(1)
        with open(SCHEMA_FILE) as f:
            schema_list = [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
    else:
        IS_ALLSCHEMA = 1
        sql = ("select nspname from pg_namespace "
               "where nspname not like 'pg%' and nspname not like 'gp%' and nspname not in ('information_schema') "
               "order by 1;")
        ret, output = run_psql([
            "psql", "-A", "-X", "-t", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Query all schema name error\n")
            sys.exit(1)
        schema_list = [s.strip() for s in output.strip().split("\n") if s.strip()]

    schema_str = "('" + "','".join(schema_list) + "')"
    print("SCHEMA: {0}".format(schema_str))
    info_notimestr("SCHEMA: {0}\n".format(schema_str))


def check_udf():
    print("---Check healthcheck UDF in DB: {0}".format(database))
    info("---Check healthcheck UDF in DB: {0}\n".format(database))

    sql = "select count(*) from pg_proc where proname in ('skewcheck_func','aotable_bloatcheck','load_files_size');"
    ret, output = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query healthcheck udf error\n")
        sys.exit(1)
    func_cnt = int(output.strip())
    if func_cnt >= 3:
        return 1
    else:
        info("UDF was not created in DB: {0}\n".format(database))
        return 0


def create_udf():
    if not FUNC_DIR:
        error("Please specified the directory of UDF scripts!\n")
        sys.exit(1)

    print("---Create healthcheck UDF in DB: {0}".format(database))
    info("---Create healthcheck UDF in DB: {0}\n".format(database))

    psql_base = ["psql", "-A", "-X", "-t", "-h", hostname, "-p", port, "-U", username, "-d", database]

    if "cbdb" in gpver:
        files = [
            "{0}/aobloat/check_ao_bloat_gp7.sql".format(FUNC_DIR),
            "{0}/gpsize/load_files_size_cbdb.sql".format(FUNC_DIR),
            "{0}/skew/skewcheck_func_gp7.sql".format(FUNC_DIR),
        ]
    elif "gp7" in gpver:
        files = [
            "{0}/aobloat/check_ao_bloat_gp7.sql".format(FUNC_DIR),
            "{0}/gpsize/load_files_size_v7.sql".format(FUNC_DIR),
            "{0}/skew/skewcheck_func_gp7.sql".format(FUNC_DIR),
        ]
    elif "gp6" in gpver:
        files = [
            "{0}/aobloat/check_ao_bloat.sql".format(FUNC_DIR),
            "{0}/gpsize/load_files_size.sql".format(FUNC_DIR),
            "{0}/skew/skewcheck_func.sql".format(FUNC_DIR),
        ]
    else:
        files = [
            "{0}/aobloat/check_ao_bloat.sql".format(FUNC_DIR),
            "{0}/gpsize/load_files_size.sql".format(FUNC_DIR),
            "{0}/skew/skewcheck_func.sql".format(FUNC_DIR),
        ]

    rets = []
    for sqlfile in files:
        proc = subprocess.Popen(psql_base + ["-f", sqlfile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        rets.append(proc.returncode)

    if any(rets):
        error("Create healthcheck UDF error!\n")
        sys.exit(1)


def gpstate():
    print("---Check gpstate and gp_configuration_history")
    info("---gpstate\n")

    proc = subprocess.Popen(["gpstate", "-e"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    info_notimestr("\n{0}\n".format(out))

    proc = subprocess.Popen(["gpstate", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    info_notimestr("{0}\n".format(out))

    sql = "select * from gp_configuration_history order by 1 desc limit 50;"
    ret, confhis = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Get gp_configuration_history error\n")
        return
    info("---gp_configuration_history\n")
    info_notimestr("{0}\n".format(confhis))


def gpclusterinfo():
    print("---Check GP cluster info")

    psql_base = ["psql", "-A", "-X", "-t", "-h", hostname, "-p", port, "-U", username, "-d", "postgres"]

    # Export hostfiles
    for label, where_clause, tmpfile in [
        ("allmasters", "where content=-1", "/tmp/tmpallmasters"),
        ("allhosts", "", "/tmp/tmpallhosts"),
        ("allsegs", "where content>-1", "/tmp/tmpallsegs"),
    ]:
        sql = "copy (select distinct address from gp_segment_configuration {0} order by 1) to '{1}';".format(where_clause, tmpfile)
        ret, _ = run_psql(psql_base + ["-c", sql])
        if ret:
            error("Export {0} error\n".format(label))
            sys.exit(1)

    # Global info
    sql = "select count(distinct hostname) from gp_segment_configuration where content>-1;"
    ret, hostcount = run_psql(psql_base + ["-c", sql])
    if ret:
        error("Get segment host count error\n")
        return
    hostcount = hostcount.strip()

    sql = "select count(*) from gp_segment_configuration where content>-1 and preferred_role='p';"
    ret, segcount = run_psql(psql_base + ["-c", sql])
    if ret:
        error("Get segment instance count error\n")
        return
    segcount = segcount.strip()

    info("---GP Cluster info\n")
    info_notimestr("Segment hosts: {0}\nPrimary segment instances: {1}\n\n".format(hostcount, segcount))


def disk_space():
    print("---Check hosts disk space")
    proc = subprocess.Popen(
        'gpssh -f /tmp/tmpallhosts "df -h 2>/dev/null |grep data"',
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if proc.returncode:
        error("Gpssh check segment space error\n")
        return
    info("---Hosts disk space\n")
    info_notimestr("{0}\n\n".format(out))


def db_size():
    print("---Check database size")
    sql = ("select datname,pg_size_pretty(pg_database_size(oid)) from pg_database "
           "where datname not in ('postgres','template1','template0');")
    ret, dbsizeinfo = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query db size error\n")
        return
    info("---Database size\n")
    info_notimestr("{0}\n\n".format(dbsizeinfo))


def chk_age():
    print("---Check database AGE")
    sql = "select datname,age(datfrozenxid) from pg_database order by 2 desc;"
    ret, master_age = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query master age error! \n")
        return

    sql = "select gp_segment_id,datname,age(datfrozenxid) from gp_dist_random('pg_database') order by 3 desc limit 50;"
    ret, seg_age = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query Segment instance age error! \n")
        return

    info("---Database AGE\n")
    info("---Master\n")
    info_notimestr("{0}\n".format(master_age))
    info("---Segment instance\n")
    info_notimestr("{0}\n".format(seg_age))

    print("---Check global xid")
    sql = "begin;select gp_distributed_xid();"
    ret, chk_gxid = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query global xid error! \n")
        return
    info("---Global xid\n")
    info_notimestr("{0}\n".format(chk_gxid))


def chk_activity():
    print("---Check pg_stat_activity")

    if gpver in ("gp6", "gp7") or "cbdb" in gpver:
        sql = ("select pid,sess_id,usename,query,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where state='idle in transaction' and "
               "(now()-xact_start>interval '1 day' or now()-state_change>interval '1 day')")
    else:
        sql = ("select procpid,sess_id,usename,current_query,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where current_query like '<IDLE> in transaction%' and "
               "(now()-xact_start>interval '1 day')")

    ret, idle_info = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query IDLE in transaction error! \n")
        return
    info("---Check IDLE in transaction over one day\n")
    info_notimestr("{0}\n".format(idle_info))

    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select pid,sess_id,usename,substr(query,1,100) query,wait_event_type,wait_event,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where state='active' and now()-query_start>interval '1 day'")
    elif gpver == "gp6":
        sql = ("select pid,sess_id,usename,substr(query,1,100) query,waiting,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where state='active' and now()-query_start>interval '1 day'")
    else:
        sql = ("select procpid,sess_id,usename,substr(current_query,1,100) current_query,waiting,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where current_query not like '%IDLE%' and now()-query_start>interval '1 day'")

    ret, query_info = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query long SQL error! \n")
        return
    info("---Check SQL running over one day\n")
    info_notimestr("{0}\n".format(query_info))


def object_size():
    print("---Load data file size on all segments")

    sql = "truncate gp_seg_size_ora; truncate gp_seg_table_size;"
    ret, _ = run_psql_simple(sql)
    if ret:
        error("Truncate gp_seg_table_size error\n")
        return

    sql = "select gp_segment_id,load_files_size() from gp_dist_random('gp_id');"
    ret, _ = run_psql_simple(sql)
    if ret:
        error("Load data file size on all segments error\n")
        return

    print("---Check Schema size")
    sql = ("with foo as (select relnamespace,sum(size)::bigint as size from gp_seg_table_size group by 1) "
           "select a.nspname,pg_size_pretty(b.size) "
           "from pg_namespace a,foo b "
           "where a.oid=b.relnamespace and a.nspname not like 'pg_temp%' "
           "order by b.size desc;")
    ret, schemasizeinfo = run_psql_simple(sql)
    if ret:
        error("Query schema size error\n")
        return
    info("---Schema size\n")
    info_notimestr("{0}\n\n".format(schemasizeinfo))

    print("---Check Tablespace size")
    sql = ("select case when spcname is null then 'pg_default' else spcname end as tsname, "
           "pg_size_pretty(tssize) "
           "from ( "
           "select c.spcname,sum(a.size)::bigint tssize "
           "from gp_seg_table_size a "
           "left join pg_tablespace c on a.reltablespace=c.oid "
           "group by 1 "
           ") foo "
           "order by tssize desc;")
    ret, tssizeinfo = run_psql_simple(sql)
    if ret:
        error("Query Tablespace size error\n")
        return
    info("---Tablespace size\n")
    info_notimestr("{0}\n\n".format(tssizeinfo))

    print("---Check Tablespace filenum")
    sql = ("select tsname,segfilenum as max_segfilenum "
           "from ( "
           "select case when spcname is null then 'pg_default' else spcname end as tsname, "
           "segfilenum, "
           "row_number() over(partition by spcname order by segfilenum desc) rn "
           "from ( "
           "select c.spcname,a.gp_segment_id segid,sum(relfilecount) segfilenum "
           "from gp_seg_table_size a "
           "left join pg_tablespace c on a.reltablespace=c.oid "
           "group by 1,2 "
           ") foo "
           ") t1 where rn=1 "
           "order by max_segfilenum desc;")
    ret, tsfilenuminfo = run_psql_simple(sql)
    if ret:
        error("Query Tablespace filenum error\n")
        return
    info("---Tablespace filenum\n")
    info_notimestr("{0}\n\n".format(tsfilenuminfo))

    print("---Check Large table top 50")
    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select b.nspname||'.'||a.relname as tablename, d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d "
               "where a.relnamespace=b.oid and a.oid=c.oid and c.relam=d.oid and c.relam in (3434,3435) "
               "group by 1,2 order by sum(a.size) desc limit 50;")
    else:
        sql = ("select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c') "
               "group by 1,2 order by sum(a.size) desc limit 50;")
    ret, aotableinfo = run_psql_simple(sql)
    if ret:
        error("Query AO table error\n")
        return
    info("---AO Table top 50\n")
    info_notimestr("{0}\n\n".format(aotableinfo))

    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select b.nspname||'.'||a.relname as tablename, d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d "
               "where a.relnamespace=b.oid and a.oid=c.oid and c.relam=d.oid and c.relam = 2 "
               "group by 1,2 order by sum(a.size) desc limit 50;")
    else:
        sql = ("select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage = 'h' "
               "group by 1,2 order by sum(a.size) desc limit 50;")
    ret, heaptableinfo = run_psql_simple(sql)
    if ret:
        error("Query Heap table error\n")
        return
    info("---Heap Table top 50\n")
    info_notimestr("{0}\n\n".format(heaptableinfo))

    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select pg_partition_root(c.oid)::regclass as root_partition, "
               "d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d "
               "where a.oid=c.oid and c.relam=d.oid and c.relam in (2,3434,3435) and c.relispartition=true "
               "group by 1,2 order by sum(a.size) desc limit 100;")
    else:
        sql = ("select substr(b.nspname||'.'||a.relname,1,position('_1_prt_' in b.nspname||'.'||a.relname)-1) as root_partition, "
               "c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c "
               "where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c','h') and position('_1_prt_' in a.relname)>0 "
               "group by 1,2 order by sum(a.size) desc limit 100;")
    ret, parttableinfo = run_psql_simple(sql)
    if ret:
        error("Query partition table size error\n")
        return
    info("---Partition Table Size top 100\n")
    info_notimestr("{0}\n\n".format(parttableinfo))

    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select b.nspname||'.'||a.relname as tablename, d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d "
               "where a.relnamespace=b.oid and a.oid=c.oid and c.relam=d.oid and c.relam in (2,3434,3435) "
               "and c.relpersistence='t' "
               "group by 1,2 order by sum(a.size) desc limit 50;")
    else:
        sql = ("select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size "
               "from gp_seg_table_size a,pg_namespace b,pg_class c "
               "where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c','h') "
               "and b.nspname like 'pg_temp%' "
               "group by 1,2 order by sum(a.size) desc limit 50;")
    ret, temptableinfo = run_psql_simple(sql)
    if ret:
        error("Query temp table size error\n")
        return
    info("---Temp Table Size top 50\n")
    info_notimestr("{0}\n\n".format(temptableinfo))

    sql = ("select schemaname||'.'||tablename tablename,pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) table_size, "
           "schemaname||'.'||indexname indexname,pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) index_size "
           "from pg_indexes order by pg_relation_size(schemaname||'.'||indexname) desc limit 50;")
    ret, indexinfo = run_psql_simple(sql)
    if ret:
        error("Query index size error\n")
        return
    info("---Index Size top 50\n")
    info_notimestr("{0}\n\n".format(indexinfo))


def chk_catalog():
    gp_session_role_name = "gp_role" if "cbdb" in gpver else "gp_session_role"

    print("---Check pg_catalog")

    # pg_tables count
    sql = "select count(*) from pg_tables;"
    ret, table_count = run_psql_simple(sql)
    if ret:
        error("pg_tables count error! \n")
        return
    table_count = table_count.strip()

    # pg_views count
    sql = "select count(*) from pg_views;"
    ret, view_count = run_psql_simple(sql)
    if ret:
        error("pg_views count error! \n")
        return
    view_count = view_count.strip()

    # pg_namespace
    sql = "select pg_size_pretty(pg_relation_size('pg_namespace'));"
    ret, pg_namespace_size = run_psql_simple(sql)
    if ret:
        print("pg_namespace size error!")
        return
    pg_namespace_size = pg_namespace_size.strip()

    sql = "select pg_size_pretty(pg_relation_size('pg_namespace')),pg_relation_size('pg_namespace');"
    env = dict(os.environ)
    env["PGOPTIONS"] = "-c {0}=utility".format(gp_session_role_name)
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("pg_namespace master size error!")
        return
    tmp_result = out.strip()
    tmpstr = tmp_result.split("|")
    pg_namespace_master = tmpstr[0]
    pg_namespace_master_int = int(tmpstr[1])

    sql = "select pg_size_pretty(pg_relation_size('pg_namespace')) from gp_dist_random('gp_id') where gp_segment_id=0;"
    ret, pg_namespace_gpseg0 = run_psql_simple(sql)
    if ret:
        print("pg_namespace gpseg0 size error!")
        return
    pg_namespace_gpseg0 = pg_namespace_gpseg0.strip()

    sql = ("create temp table tmp_pg_namespace_record as select * from pg_namespace;\n"
           "select pg_relation_size('tmp_pg_namespace_record');")
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-q", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("pg_namespace realsize error!")
        return
    pg_namespace_realsize = int(out.strip())
    pg_namespace_master_bloat = pg_namespace_master_int / pg_namespace_realsize if pg_namespace_realsize else 0

    sql = "select count(*) from pg_namespace;"
    ret, pg_namespace_count = run_psql_simple(sql)
    if ret:
        print("pg_namespace count error!")
        return
    pg_namespace_count = pg_namespace_count.strip()

    # pg_class
    sql = "select pg_size_pretty(pg_relation_size('pg_class'));"
    ret, pg_class_size = run_psql_simple(sql)
    if ret:
        error("pg_class size error! \n")
        return
    pg_class_size = pg_class_size.strip()

    sql = "select pg_size_pretty(pg_relation_size('pg_class')),pg_relation_size('pg_class');"
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        error("pg_class master size error! \n")
        return
    tmp_result = out.strip()
    tmpstr = tmp_result.split("|")
    pg_class_master = tmpstr[0]
    pg_class_master_int = int(tmpstr[1])

    sql = "select pg_size_pretty(pg_relation_size('pg_class')) from gp_dist_random('gp_id') where gp_segment_id=0;"
    ret, pg_class_gpseg0 = run_psql_simple(sql)
    if ret:
        error("pg_class gpseg0 size error! \n")
        return
    pg_class_gpseg0 = pg_class_gpseg0.strip()

    sql = ("create temp table tmp_pg_class_record as select * from pg_class;\n"
           "select pg_relation_size('tmp_pg_class_record');")
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-q", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("pg_class realsize error!")
        return
    pg_class_realsize = int(out.strip())
    pg_class_master_bloat = pg_class_master_int / pg_class_realsize if pg_class_realsize else 0

    sql = "select count(*) from pg_class;"
    ret, pg_class_count = run_psql_simple(sql)
    if ret:
        error("pg_class count error! \n")
        return
    pg_class_count = pg_class_count.strip()

    # pg_attribute
    sql = "select pg_size_pretty(pg_relation_size('pg_attribute'));"
    ret, pg_attribute_size = run_psql_simple(sql)
    if ret:
        error("pg_attribute size error! \n")
        return
    pg_attribute_size = pg_attribute_size.strip()

    sql = "select pg_size_pretty(pg_relation_size('pg_attribute')),pg_relation_size('pg_attribute');"
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        error("pg_attribute master size error! \n")
        return
    tmp_result = out.strip()
    tmpstr = tmp_result.split("|")
    pg_attribute_master = tmpstr[0]
    pg_attribute_master_int = int(tmpstr[1])

    sql = "select pg_size_pretty(pg_relation_size('pg_attribute')) from gp_dist_random('gp_id') where gp_segment_id=0;"
    ret, pg_attribute_gpseg0 = run_psql_simple(sql)
    if ret:
        error("pg_attribute gpseg0 size error! \n")
        return
    pg_attribute_gpseg0 = pg_attribute_gpseg0.strip()

    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("create temp table tmp_pg_attribute_record ("
               "attrelid oid, attname name, atttypid oid, attstattarget integer, "
               "attlen smallint, attnum smallint, attndims integer, attcacheoff integer, "
               "atttypmod integer, attbyval boolean, attstorage \"char\", attalign \"char\", "
               "attnotnull boolean, atthasdef boolean, atthasmissing boolean, "
               "attidentity \"char\", attgenerated \"char\", attisdropped boolean, "
               "attislocal boolean, attinhcount integer, attcollation oid, "
               "attacl aclitem[], attoptions text[], attfdwoptions text[]);\n"
               "insert into tmp_pg_attribute_record "
               "select attrelid,attname,atttypid,attstattarget,attlen,attnum,attndims,attcacheoff,"
               "atttypmod,attbyval,attstorage,attalign,attnotnull,atthasdef,"
               "atthasmissing,attidentity,attgenerated,attisdropped,attislocal,attinhcount,"
               "attcollation,attacl,attoptions,attfdwoptions from pg_attribute;\n"
               "select pg_relation_size('tmp_pg_attribute_record');")
    else:
        sql = ("create temp table tmp_pg_attribute_record as select * from pg_attribute;\n"
               "select pg_relation_size('tmp_pg_attribute_record');")
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-q", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("pg_attribute realsize error!")
        return
    pg_attribute_realsize = int(out.strip())
    pg_attribute_master_bloat = pg_attribute_master_int / pg_attribute_realsize if pg_attribute_realsize else 0

    sql = "select count(*) from pg_attribute;"
    ret, pg_attribute_count = run_psql_simple(sql)
    if ret:
        error("pg_attribute count error! \n")
        return
    pg_attribute_count = pg_attribute_count.strip()

    # partition count
    if gpver == "gp7" or "cbdb" in gpver:
        sql = "select count(*) from pg_class where relispartition = true;"
    else:
        sql = "select count(*) from pg_partition_rule;"
    ret, partition_count = run_psql_simple(sql)
    if ret:
        error("pg_partition_rule count error! \n")
        return
    partition_count = partition_count.strip()

    info("---pg_catalog info\n")
    info_notimestr("pg_tables count:               {0}\n".format(table_count))
    info_notimestr("pg_views count:                {0}\n".format(view_count))
    info_notimestr("pg_namespace count:            {0}\n".format(pg_namespace_count))
    info_notimestr("pg_namespace size:             {0}\n".format(pg_namespace_size))
    info_notimestr("pg_namespace size in master:   {0}\n".format(pg_namespace_master))
    info_notimestr("pg_namespace size in gpseg0:   {0}\n".format(pg_namespace_gpseg0))
    info_notimestr("pg_namespace bloat in master:  {0}\n".format(pg_namespace_master_bloat))
    info_notimestr("pg_class count:                {0}\n".format(pg_class_count))
    info_notimestr("pg_class size:                 {0}\n".format(pg_class_size))
    info_notimestr("pg_class size in master:       {0}\n".format(pg_class_master))
    info_notimestr("pg_class size in gpseg0:       {0}\n".format(pg_class_gpseg0))
    info_notimestr("pg_class bloat in master:      {0}\n".format(pg_class_master_bloat))
    info_notimestr("pg_attribute count:            {0}\n".format(pg_attribute_count))
    info_notimestr("pg_attribute size:             {0}\n".format(pg_attribute_size))
    info_notimestr("pg_attribute size in master:   {0}\n".format(pg_attribute_master))
    info_notimestr("pg_attribute size in gpseg0:   {0}\n".format(pg_attribute_gpseg0))
    info_notimestr("pg_attribute bloat in master:  {0}\n".format(pg_attribute_master_bloat))
    info_notimestr("partition count:               {0}\n".format(partition_count))
    info_notimestr("\n")

    # Table type info per schema
    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select a.nspname schemaname,c.amname tabletype,count(*) "
               "from pg_namespace a,pg_class b,pg_am c "
               "where a.oid=b.relnamespace and b.relam=c.oid and relkind in ('r','p') and a.nspname not like 'pg%' and a.nspname not like 'gp%' "
               "group by 1,2 "
               "union all "
               "select a.nspname schemaname,'foreign table' tabletype,count(*) "
               "from pg_namespace a,pg_class b "
               "where a.oid=b.relnamespace and relkind='f' and a.nspname not like 'pg%' and a.nspname not like 'gp%' "
               "group by 1,2 "
               "order by 1,2;")
    else:
        sql = ("select a.nspname schemaname, "
               "case when b.relstorage='a' then 'AO row' "
               "when b.relstorage='c' then 'AO column' "
               "when b.relstorage='h' then 'Heap' "
               "when b.relstorage='x' then 'External' "
               "else 'Others' end tabletype, "
               "count(*) "
               "from pg_namespace a,pg_class b "
               "where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%' "
               "group by 1,2 order by 1,2;")

    ret, tabletype = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Table type count per schema error! \n")
        return
    info("---Table type info per schema\n")
    info_notimestr("{0}\n".format(tabletype))

    # Table type total
    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select c.amname tabletype, count(*) "
               "from pg_namespace a,pg_class b,pg_am c "
               "where a.oid=b.relnamespace and b.relam=c.oid and relkind in ('r','p') and a.nspname not like 'pg%' and a.nspname not like 'gp%' "
               "group by 1 "
               "union all "
               "select 'foreign table' tabletype, count(*) "
               "from pg_namespace a,pg_class b "
               "where a.oid=b.relnamespace and b.relkind='f' and a.nspname not like 'pg%' and a.nspname not like 'gp%' "
               "order by 1;")
    else:
        sql = ("select "
               "case when b.relstorage='a' then 'AO row' "
               "when b.relstorage='c' then 'AO column' "
               "when b.relstorage='h' then 'Heap' "
               "when b.relstorage='x' then 'External' "
               "else 'Others' end tabletype, "
               "count(*) "
               "from pg_namespace a,pg_class b "
               "where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%' "
               "group by 1 order by 1;")

    ret, tabletype = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Table type count error! \n")
        return
    info("---Table type info\n")
    info_notimestr("{0}\n".format(tabletype))

    # pg_stat_operations
    sql = "select * from pg_stat_operations where objid in (1249,1259) order by objname,statime;"
    ret, stat_ops = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Check pg_stat_operations of pg_class/pg_attribute error! \n")
        return
    info("---Check pg_stat_operations info\n")
    info_notimestr("{0}\n".format(stat_ops))


def chk_partition_info():
    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("SELECT tablename,COUNT(*) FROM ("
               "SELECT pg_partition_root(c.oid)::regclass AS tablename, "
               "c.oid::regclass AS partitiontablename "
               "FROM pg_class c "
               "WHERE c.relispartition = true "
               ") foo GROUP BY 1 ORDER BY 2 DESC;")
        ret, subpart = run_psql([
            "psql", "-X", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Subpartition count error! \n")
            return
        info("---Subpartition info\n")
        info_notimestr("{0}\n".format(subpart))
    else:
        sql = ("select schemaname||'.'||tablename as tablename,count(*) as sub_count from pg_partitions "
               "group by 1 order by 2 desc limit 100;")
        ret, subpart = run_psql([
            "psql", "-X", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Subpartition count error! \n")
            return
        info("---Subpartition info\n")
        info_notimestr("{0}\n".format(subpart))

        sql = ("select schemaname||'.'||tablename as tablename,partitionschemaname||'.'||partitiontablename as partitiontablename "
               "from pg_partitions where schemaname<>partitionschemaname order by 1,2;")
        ret, part_schema = run_psql([
            "psql", "-X", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Check partition schema error! \n")
            return
        info("---Check partition schema\n")
        info_notimestr("{0}\n".format(part_schema))


def _skew_worker(schema, h, p, u, d):
    """Child process worker for skew check."""
    sql = "copy (select * from skewcheck_func('{0}')) to '/tmp/tmpskew.{0}.dat';".format(schema)
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if proc.returncode:
        sys.exit(-1)
    sql = "copy check_skew_result from '/tmp/tmpskew.{0}.dat';".format(schema)
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    proc.communicate()


def skewcheck():
    print("---Begin to check skew, jobs [{0}]".format(concurrency))

    sql = ("drop table if exists check_skew_result; "
           "create table check_skew_result("
           "tablename text, sys_segcount int, data_segcount int, "
           "maxsize_segid int, maxsize text, skew numeric(18,2), dk text"
           ") distributed randomly;")
    ret, _ = run_psql_simple(sql)
    if ret:
        error("recreate check_skew_result error! \n")
        return

    itotal = len(schema_list)
    processes = []

    for icalc in range(itotal):
        while len([p for p in processes if p.is_alive()]) >= concurrency:
            time.sleep(1)

        p = Process(target=_skew_worker, args=(schema_list[icalc], hostname, port, username, database))
        p.start()
        processes.append(p)
        active = len([pp for pp in processes if pp.is_alive()])
        finished = len([pp for pp in processes if not pp.is_alive()])
        print("Child process count [{0}], finish count[{1}/{2}]".format(active, finished, itotal))

    for p in processes:
        p.join()

    print("Child process count [0], finish count[{0}/{0}]".format(itotal))

    sql = "select * from check_skew_result order by tablename,skew desc;"
    ret, skewresult = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query skew check result error! \n")
        return
    info("---Skew check\n")
    info_notimestr("\n{0}\n".format(skewresult))


def _bloat_worker(schema, h, p, u, d):
    """Child process worker for AO bloat check."""
    sql = "copy (select schemaname||'.'||tablename,'ao',bloat from AOtable_bloatcheck('{0}') where bloat>1.9) to '/tmp/tmpaobloat.{0}.dat';".format(schema)
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if proc.returncode:
        sys.exit(-1)
    sql = "copy bloat_skew_result from '/tmp/tmpaobloat.{0}.dat';".format(schema)
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    proc.communicate()


def bloatcheck():
    print("---Begin to check bloat, jobs [{0}]".format(concurrency))

    sql = ("drop table if exists bloat_skew_result; "
           "create table bloat_skew_result("
           "tablename text, relstorage varchar(10), bloat numeric(18,2)"
           ") distributed randomly;")
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        error("recreate bloat_skew_result error! \n")
        return

    if gpver == "gp7" or "cbdb" in gpver:
        pg_class_sql = "insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relam=2;"
    else:
        pg_class_sql = "insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';"

    # Heap table bloat check
    sql = ("drop table if exists pg_stats_bloat_chk; "
           "create temp table pg_stats_bloat_chk "
           "(schemaname varchar(80), tablename varchar(80), attname varchar(100), "
           "null_frac float4, avg_width int4, n_distinct float4) distributed by (tablename); "
           "drop table if exists pg_class_bloat_chk; "
           "create temp table pg_class_bloat_chk (like pg_class) distributed by (relname); "
           "drop table if exists pg_namespace_bloat_chk; "
           "create temp table pg_namespace_bloat_chk "
           "(oid_ss integer, nspname varchar(80), nspowner integer) distributed by (oid_ss); "
           "insert into pg_stats_bloat_chk "
           "select schemaname,tablename,attname,null_frac,avg_width,n_distinct from pg_stats; "
           "{0} "
           "insert into pg_namespace_bloat_chk "
           "select oid,nspname,nspowner from pg_namespace where nspname in {1}; "
           "insert into bloat_skew_result "
           "SELECT schemaname||'.'||tablename,'h',bloat "
           "FROM ( "
           "SELECT current_database() as datname, 'table' as tabletype, schemaname, tablename, "
           "reltuples::bigint AS tuples, rowsize::float::bigint AS rowsize, "
           "live_size_blocks*bs as total_size_tuples, bs*relpages::bigint AS total_size_pages, "
           "ROUND(CASE WHEN live_size_blocks = 0 AND relpages > 0 THEN 1000.0 "
           "ELSE sml.relpages/live_size_blocks::numeric END, 1) AS bloat, "
           "CASE WHEN relpages < live_size_blocks THEN 0::bigint "
           "ELSE (bs*(relpages-live_size_blocks))::bigint END AS wastedsize "
           "FROM ( "
           "SELECT schemaname, tablename, cc.reltuples, cc.relpages, bs, "
           "CEIL((cc.reltuples*((datahdr + maxalign - (CASE WHEN datahdr%maxalign = 0 THEN maxalign ELSE datahdr%maxalign END)) + nullhdr2 + 4))/(bs-20::float)) AS live_size_blocks, "
           "((datahdr + maxalign - (CASE WHEN datahdr%maxalign = 0 THEN maxalign ELSE datahdr%maxalign END)) + nullhdr2 + 4) as rowsize "
           "FROM ( "
           "SELECT maxalign, bs, schemaname, tablename, "
           "(datawidth + (hdr + maxalign - (case when hdr % maxalign = 0 THEN maxalign ELSE hdr%maxalign END)))::numeric AS datahdr, "
           "(maxfracsum * (nullhdr + maxalign - (case when nullhdr%maxalign = 0 THEN maxalign ELSE nullhdr%maxalign END))) AS nullhdr2 "
           "FROM ( "
           "SELECT med.schemaname, med.tablename, hdr, maxalign, bs, datawidth, maxfracsum, "
           "hdr + 1 + coalesce(cntt1.cnt,0) as nullhdr "
           "FROM ( "
           "SELECT schemaname, tablename, hdr, maxalign, bs, "
           "SUM((1-s.null_frac)*s.avg_width) AS datawidth, "
           "MAX(s.null_frac) AS maxfracsum "
           "FROM pg_stats_bloat_chk s, "
           "(SELECT current_setting('block_size')::numeric AS bs, 27 AS hdr, 4 AS maxalign) AS constants "
           "GROUP BY 1, 2, 3, 4, 5 "
           ") AS med "
           "LEFT JOIN ( "
           "select (count(*)/8) AS cnt,schemaname,tablename from pg_stats_bloat_chk where null_frac <> 0 group by schemaname,tablename "
           ") AS cntt1 "
           "ON med.schemaname = cntt1.schemaname and med.tablename = cntt1.tablename "
           ") AS foo "
           ") AS rs "
           "JOIN pg_class_bloat_chk cc ON cc.relname = rs.tablename "
           "JOIN pg_namespace_bloat_chk nn ON cc.relnamespace = nn.oid_ss AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' "
           ") AS sml "
           "WHERE sml.relpages - live_size_blocks > 2 "
           ") AS blochk where wastedsize>104857600 and bloat>2;").format(pg_class_sql, schema_str)

    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        error("Heap table bloat check error! \n{0}\n".format(out))
        return

    # AO table bloat check with parallel workers
    itotal = len(schema_list)
    processes = []

    for icalc in range(itotal):
        while len([p for p in processes if p.is_alive()]) >= concurrency:
            time.sleep(1)

        p = Process(target=_bloat_worker, args=(schema_list[icalc], hostname, port, username, database))
        p.start()
        processes.append(p)
        active = len([pp for pp in processes if pp.is_alive()])
        finished = len([pp for pp in processes if not pp.is_alive()])
        print("Child process count [{0}], finish count[{1}/{2}]".format(active, finished, itotal))

    for p in processes:
        p.join()

    print("Child process count [0], finish count[{0}/{0}]".format(itotal))

    sql = "select * from bloat_skew_result order by relstorage,bloat desc;"
    ret, bloatresult = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query bloat check result error! \n")
        return
    info("---Bloat check\n")
    info_notimestr("\n{0}\n".format(bloatresult))

    # Generate bloat fix script
    sql = "select count(*) from bloat_skew_result;"
    ret, bloatcount_str = run_psql_simple(sql)
    if ret:
        error("Query bloat table count error! \n")
        return
    bloatcount = int(bloatcount_str.strip())
    logday = get_current_date()
    if bloatcount > 0:
        sql = ("copy (select 'alter table '||tablename||' set with (reorganize=true); analyze '||tablename||';' from bloat_skew_result) "
               "to '{0}/fix_ao_table_script_{1}_{2}.sql';").format(LOG_DIR, database, logday)
        ret, _ = run_psql_simple(sql)
        if ret:
            error("Unload bloat table fix script error! \n")
            return
        info_notimestr("\nPlease check fix script: {0}/fix_ao_table_script_{1}_{2}.sql\n".format(LOG_DIR, database, logday))


def _defpart_worker(tablename, h, p, u, d):
    """Child process worker for default partition count."""
    sql = "insert into def_partition_count_result select '{0}',count(*) from {0};".format(tablename)
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    proc.communicate()


def def_partition():
    print("---Begin to check default partition, jobs [{0}]".format(concurrency))

    if gpver == "gp7" or "cbdb" in gpver:
        sql = ("select c.nspname||'.'||b.relname from pg_partitioned_table a,pg_class b,pg_namespace c "
               "where a.partdefid=b.oid and b.relnamespace=c.oid and b.relkind='r' and a.partdefid>0 and c.nspname in {0};").format(schema_str)
    else:
        sql = "select partitionschemaname||'.'||partitiontablename from pg_partitions where partitionisdefault=true and partitionschemaname in {0};".format(schema_str)

    ret, output = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query default partition table list error! \n")
        return
    defpart_list = [s.strip() for s in output.strip().split("\n") if s.strip()]
    itotal = len(defpart_list)

    sql = ("drop table if exists def_partition_count_result; "
           "create table def_partition_count_result("
           "tablename text, row_count bigint"
           ") distributed randomly;")
    ret, _ = run_psql_simple(sql)
    if ret:
        error("recreate def_partition_count_result error! \n")
        return

    processes = []

    for icalc in range(itotal):
        while len([p for p in processes if p.is_alive()]) >= concurrency:
            time.sleep(1)

        p = Process(target=_defpart_worker, args=(defpart_list[icalc], hostname, port, username, database))
        p.start()
        processes.append(p)
        active = len([pp for pp in processes if pp.is_alive()])
        finished = len([pp for pp in processes if not pp.is_alive()])
        print("Child process count [{0}], finish count[{1}/{2}]".format(active, finished, itotal))

    for p in processes:
        p.join()

    print("Child process count [0], finish count[{0}/{0}]".format(itotal))

    sql = "select * from def_partition_count_result where row_count>0 order by row_count desc;"
    ret, defpartresult = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query default partition count result error! \n")
        return
    info("---Default partition check\n")
    info_notimestr("\n{0}\n".format(defpartresult))


def chk_os_param():
    print("---Check OS parameter")

    checks = [
        ('cat /etc/sysctl.conf |grep -vE \'^\\s*#|^\\s*$\'', "Check /etc/sysctl.conf", "Gpssh check sysctl.conf error\n", False),
        ('ulimit -a', "Check ulimit", "Gpssh check ulimit error\n", True),
        ('mount |grep xfs', "Check mount info", "Gpssh check mount info\n", True),
        ('cat /sys/kernel/mm/transparent_hugepage/enabled', "Check hugepage", "Gpssh check hugepage \n", True),
        ('date', "Check system clock", "Gpssh check system clock \n", True),
    ]

    for cmd, label, err_msg, do_return in checks:
        proc = subprocess.Popen(
            'gpssh -d 0 -f /tmp/tmpallhosts "{0}"'.format(cmd),
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if hasattr(out, 'decode'):
            out = out.decode('utf-8', 'replace')
        if hasattr(err, 'decode'):
            err = err.decode('utf-8', 'replace')
        if proc.returncode:
            error(err_msg)
            if do_return:
                return
        info("---{0} ...\n".format(label))
        info_notimestr("{0}\n\n".format(out))


def chk_gpdb_param():
    print("---Check GPDB parameter")

    if gpver == "gp7" or "cbdb" in gpver:
        master_dir = os.environ.get("COORDINATOR_DATA_DIRECTORY", "")
    else:
        master_dir = os.environ.get("MASTER_DATA_DIRECTORY", "")

    proc = subprocess.Popen(
        "cat {0}/postgresql.conf | grep -vE '^\\s*#|^\\s*$'".format(master_dir),
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        error("Check postgresql.conf error\n")
        return
    info("---Check setting in postgresql.conf ...\n")
    info_notimestr("{0}\n\n".format(out))

    if gpver in ("gp6", "gp7") or "cbdb" in gpver:
        sql = ("select a.datname,array_to_string(b.setconfig,',') db_setting "
               "from pg_database a,pg_db_role_setting b where a.oid=b.setdatabase and b.setrole=0;")
    else:
        sql = "select datname,array_to_string(datconfig,',') db_setting from pg_database where datconfig is not null;"
    ret, param_info = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query setting on database error! \n")
        return
    info("---Check setting on database ...\n")
    info_notimestr("{0}\n".format(param_info))

    if gpver in ("gp6", "gp7") or "cbdb" in gpver:
        sql = ("select a.rolname,array_to_string(b.setconfig,',') role_setting "
               "from pg_roles a,pg_db_role_setting b where a.oid=b.setrole and b.setdatabase=0;")
    else:
        sql = "select rolname,array_to_string(rolconfig,',') role_setting from pg_roles where rolconfig is not null;"
    ret, param_info = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query setting on role error! \n")
        return
    info("---Check setting on role ...\n")
    info_notimestr("{0}\n".format(param_info))


def main():
    global hostname, port, database, username, password
    global IS_ALLDB, IS_ALLSCHEMA, IS_SKIPUDF, FUNC_DIR
    global CHK_SCHEMA, SCHEMA_FILE, concurrency, LOG_DIR, gpver

    parser = argparse.ArgumentParser(
        description="GPDB Health Check Tool",
        add_help=False
    )
    parser.add_argument("--hostname", "-h", default="localhost")
    parser.add_argument("--port", "-p", default="5432")
    parser.add_argument("--dbname", default=None)
    parser.add_argument("--username", default="gpadmin")
    parser.add_argument("--password", "--pw", default="gpadmin")
    parser.add_argument("--help", "-?", action="store_true", dest="show_help")
    parser.add_argument("--alldb", "-A", action="store_true")
    parser.add_argument("--include-schema", "-s", action="append", default=[])
    parser.add_argument("--include-schema-file", default="")
    parser.add_argument("--jobs", default="2")
    parser.add_argument("--log-dir", "-l", default=None)
    parser.add_argument("--skip-without-udf", action="store_true")
    parser.add_argument("--create-udf", default="")

    if len(sys.argv) == 1:
        print("Input error: \nPlease show help: python3 {0} --help".format(cmd_name))
        sys.exit(0)

    args = parser.parse_args()

    if args.show_help:
        parser.print_help()
        sys.exit(0)

    hostname = args.hostname
    port = args.port
    database = args.dbname if args.dbname else ""
    username = args.username
    password = args.password
    IS_ALLDB = args.alldb
    CHK_SCHEMA = args.include_schema
    SCHEMA_FILE = args.include_schema_file
    concurrency = int(args.jobs) if args.jobs else 2
    IS_SKIPUDF = args.skip_without_udf
    FUNC_DIR = args.create_udf

    home_dir = os.environ.get("HOME", "")
    LOG_DIR = args.log_dir if args.log_dir else "{0}/gpAdminLogs".format(home_dir)

    print("LOG Directory: {0}".format(LOG_DIR))

    if IS_ALLDB and database:
        print("Input error: The following options may not be specified together: --alldb, --dbname <database_name>")
        sys.exit(0)
    elif not IS_ALLDB and not database:
        pgdatabase = os.environ.get("PGDATABASE", "")
        if pgdatabase:
            database = pgdatabase
        else:
            print("Input error: Please specify one of this options: --alldb or --dbname <database_name>")
            sys.exit(0)

    itmp = 0
    if CHK_SCHEMA:
        itmp += 1
    if SCHEMA_FILE:
        itmp += 1
    if IS_ALLDB and itmp > 0:
        print("Input error: The option --alldb may not be specified with include-schema, include-schema-file")
        sys.exit(0)
    if itmp > 1:
        print("Input error: The following options may not be specified together: include-schema, include-schema-file")
        sys.exit(0)

    if concurrency <= 0:
        print("Input error: --jobs <parallel_job_number>\n  The number of parallel jobs to healthcheck. Default value: 2")
        sys.exit(0)

    if IS_SKIPUDF and FUNC_DIR:
        print("Input error: The following options may not be specified together: skip-udf, auto-create-udf")
        sys.exit(0)

    set_env()
    init_log()
    info("-----------------------------------------------------\n")
    info("------Begin GPDB health check\n")
    info("-----------------------------------------------------\n")
    info_notimestr("Hostname: {0}\nPort: {1}\nUsername: {2}\nConcurrency: {3}\nLogDIR: {4}\n".format(hostname, port, username, concurrency, LOG_DIR))
    gpver = get_gpver()
    info("-----------------------------------------------------\n")
    get_dbname()

    # GP cluster info
    gpstate()
    gpclusterinfo()
    disk_space()
    db_size()
    chk_age()
    chk_activity()

    for i in range(len(dbname_list)):
        database = dbname_list[i].strip()
        print("------Begin to check database: {0}".format(database))
        info("-----------------------------------------------------\n")
        info("------Begin to check database: {0}\n".format(database))
        info("-----------------------------------------------------\n")
        get_schema()
        has_udf = check_udf()
        if not has_udf and FUNC_DIR:
            create_udf()
        if IS_SKIPUDF and not has_udf:
            chk_catalog()
            chk_partition_info()
            def_partition()
        else:
            chk_catalog()
            object_size()
            chk_partition_info()
            skewcheck()
            bloatcheck()
            def_partition()

    info("-----------------------------------------------------\n")
    info("------Begin to check OS parameter\n")
    info("-----------------------------------------------------\n")
    chk_os_param()

    info("-----------------------------------------------------\n")
    info("------Begin to check GPDB parameter\n")
    info("-----------------------------------------------------\n")
    chk_gpdb_param()

    info("-----------------------------------------------------\n")
    info("------Finished GPDB health check!\n")
    info("-----------------------------------------------------\n\n\n")
    close_log()


if __name__ == "__main__":
    main()
