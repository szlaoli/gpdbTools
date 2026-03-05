#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from multiprocessing import Process, Value
from typing import IO, Optional


cmd_name = os.path.basename(sys.argv[0])
hostname: str = "localhost"
port: str = "5432"
database: str = ""
username: str = "gpadmin"
password: str = "gpadmin"
IS_ALLDB: bool = False
IS_ALLSCHEMA: int = 0
IS_SKIPUDF: bool = False
FUNC_DIR: str = ""
CHK_SCHEMA: list[str] = []
SCHEMA_FILE: str = ""
concurrency: int = 2
LOG_DIR: str = ""
fh_log: Optional[IO[str]] = None
schema_list: list[str] = []
schema_str: str = ""
gpver: str = ""
dbname_list: list[str] = []


def get_current_date() -> str:
    return datetime.now().strftime("%Y%m%d")


def show_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_log() -> None:
    global fh_log
    logday = get_current_date()
    if LOG_DIR == "~/gpAdminLogs":
        logpath = f"{os.environ['HOME']}/gpAdminLogs/{cmd_name}_{logday}.log"
    else:
        logpath = f"{LOG_DIR}/{cmd_name}_{logday}.log"
    try:
        fh_log = open(logpath, "a")
    except OSError:
        print(f"[ERROR]:Cound not open logfile {logpath}")
        sys.exit(-1)


def info(printmsg: str) -> int:
    if fh_log:
        fh_log.write(f"[{show_time()} INFO] {printmsg}")
    return 0


def info_notimestr(printmsg: str) -> int:
    if fh_log:
        fh_log.write(printmsg)
    return 0


def error(printmsg: str) -> int:
    if fh_log:
        fh_log.write(f"[{show_time()} ERROR] {printmsg}")
    return 0


def close_log() -> None:
    if fh_log:
        fh_log.close()


def set_env() -> None:
    os.environ["PGHOST"] = hostname
    os.environ["PGPORT"] = port
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password


def run_psql(args: list[str]) -> tuple[int, str]:
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode, result.stdout


def run_psql_simple(sql: str, extra_flags: str = "-A -X -t") -> tuple[int, str]:
    flags = extra_flags.split()
    args = ["psql"] + flags + ["-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database]
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode, result.stdout


def get_gpver() -> str:
    sql = "select version();"
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-d", "postgres"],
        capture_output=True, text=True
    )
    if result.returncode:
        print("Get GP version error!")
        sys.exit(1)
    sver = result.stdout.strip()

    if "Greenplum Database" in sver:
        tmpstr = sver.split(" ")
        print(tmpstr[4])
        info_notimestr(f"GP Version: {tmpstr[4]}\n")
        tmpver = tmpstr[4].split(".")
        sresult = f"gp{tmpver[0]}"
        print(sresult)
    elif "Cloudberry Database" in sver or "Apache Cloudberry" in sver:
        tmpstr = sver.split(" ")
        print(tmpstr[4])
        info_notimestr(f"CBDB Version: {tmpstr[4]}\n")
        sresult = f"cbdb{tmpstr[4]}"
        print(sresult)
    else:
        sresult = ""

    return sresult


def get_dbname() -> None:
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


def get_schema() -> None:
    global schema_list, schema_str, IS_ALLSCHEMA

    if CHK_SCHEMA:
        schema_list = list(CHK_SCHEMA)
    elif SCHEMA_FILE:
        if not os.path.exists(SCHEMA_FILE):
            error(f"Schema file {SCHEMA_FILE} do not exist!\n")
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

    parts = [f"'{s}'" for s in schema_list]
    schema_str = "(" + ",".join(parts) + ")"
    print(f"SCHEMA: {schema_str}")
    info_notimestr(f"SCHEMA: {schema_str}\n")


def check_udf() -> int:
    print(f"---Check healthcheck UDF in DB: {database}")
    info(f"---Check healthcheck UDF in DB: {database}\n")

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
        info(f"UDF was not created in DB: {database}\n")
        return 0


def create_udf() -> None:
    if not FUNC_DIR:
        error("Please specified the directory of UDF scripts!\n")
        sys.exit(1)

    print(f"---Create healthcheck UDF in DB: {database}")
    info(f"---Create healthcheck UDF in DB: {database}\n")

    psql_base = ["psql", "-A", "-X", "-t", "-h", hostname, "-p", port, "-U", username, "-d", database]

    if "cbdb" in gpver:
        files = [
            f"{FUNC_DIR}/aobloat/check_ao_bloat_gp7.sql",
            f"{FUNC_DIR}/gpsize/load_files_size_cbdb.sql",
            f"{FUNC_DIR}/skew/skewcheck_func_gp7.sql",
        ]
    elif "gp7" in gpver:
        files = [
            f"{FUNC_DIR}/aobloat/check_ao_bloat_gp7.sql",
            f"{FUNC_DIR}/gpsize/load_files_size_v7.sql",
            f"{FUNC_DIR}/skew/skewcheck_func_gp7.sql",
        ]
    elif "gp6" in gpver:
        files = [
            f"{FUNC_DIR}/aobloat/check_ao_bloat.sql",
            f"{FUNC_DIR}/gpsize/load_files_size_v6.sql",
            f"{FUNC_DIR}/skew/skewcheck_func_gp6.sql",
        ]
    else:
        files = [
            f"{FUNC_DIR}/aobloat/check_ao_bloat.sql",
            f"{FUNC_DIR}/gpsize/load_files_size.sql",
            f"{FUNC_DIR}/skew/skewcheck_func.sql",
        ]

    rets = []
    for sqlfile in files:
        result = subprocess.run(psql_base + ["-f", sqlfile], capture_output=True, text=True)
        rets.append(result.returncode)

    if any(rets):
        error("Create healthcheck UDF error!\n")
        sys.exit(1)


def gpstate() -> None:
    print("---Check gpstate and gp_configuration_history")
    info("---gpstate\n")

    result = subprocess.run(["gpstate", "-e"], capture_output=True, text=True)
    info_notimestr(f"\n{result.stdout}\n")
    result = subprocess.run(["gpstate", "-f"], capture_output=True, text=True)
    info_notimestr(f"{result.stdout}\n")

    sql = "select * from gp_configuration_history order by 1 desc limit 50;"
    ret, confhis = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Get gp_configuration_history error\n")
        return
    info("---gp_configuration_history\n")
    info_notimestr(f"{confhis}\n")


def gpclusterinfo() -> None:
    print("---Check GP cluster info")

    psql_base = ["psql", "-A", "-X", "-t", "-h", hostname, "-p", port, "-U", username, "-d", "postgres"]

    # Export hostfiles
    for label, where_clause, tmpfile in [
        ("allmasters", "where content=-1", "/tmp/tmpallmasters"),
        ("allhosts", "", "/tmp/tmpallhosts"),
        ("allsegs", "where content>-1", "/tmp/tmpallsegs"),
    ]:
        sql = f"copy (select distinct address from gp_segment_configuration {where_clause} order by 1) to '{tmpfile}';"
        ret, _ = run_psql(psql_base + ["-c", sql])
        if ret:
            error(f"Export tmp {label} error\n")
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
    info_notimestr(f"Segment hosts: {hostcount}\nPrimary segment instances: {segcount}\n\n")


def disk_space() -> None:
    print("---Check hosts disk space")
    result = subprocess.run(
        'gpssh -f /tmp/tmpallhosts "df -h 2>/dev/null |grep data"',
        shell=True, capture_output=True, text=True
    )
    if result.returncode:
        error("Gpssh check segment space error\n")
        return
    info("---Hosts disk space\n")
    info_notimestr(f"{result.stdout}\n\n")


def db_size() -> None:
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
    info_notimestr(f"{dbsizeinfo}\n\n")


def chk_age() -> None:
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
    info_notimestr(f"{master_age}\n")
    info("---Segment instance\n")
    info_notimestr(f"{seg_age}\n")

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
    info_notimestr(f"{chk_gxid}\n")


def chk_activity() -> None:
    print("---Check pg_stat_activity")

    if gpver in ("gp6", "gp7") or "cbdb" in gpver:
        sql = ("select pid,sess_id,usename,query,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where state='idle in transaction' and "
               "(now()-xact_start>interval '1 day' or now()-state_change>interval '1 day')")
    else:
        sql = ("select procpid,sess_id,usename,current_query,query_start,xact_start,backend_start,client_addr "
               "from pg_stat_activity where current_query='<IDLE> in transaction' and "
               "(now()-xact_start>interval '1 day' or now()-query_start>interval '1 day')")

    ret, idle_info = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", "postgres"
    ])
    if ret:
        error("Query IDLE in transaction error! \n")
        return
    info("---Check IDLE in transaction over one day\n")
    info_notimestr(f"{idle_info}\n")

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
    info_notimestr(f"{query_info}\n")


def object_size() -> None:
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
    info_notimestr(f"{schemasizeinfo}\n\n")

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
    info_notimestr(f"{tssizeinfo}\n\n")

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
    info_notimestr(f"{tsfilenuminfo}\n\n")

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
    info_notimestr(f"{aotableinfo}\n\n")

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
    info_notimestr(f"{heaptableinfo}\n\n")

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
    info_notimestr(f"{parttableinfo}\n\n")

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
    info_notimestr(f"{temptableinfo}\n\n")

    sql = ("select schemaname||'.'||tablename tablename,pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) table_size, "
           "schemaname||'.'||indexname indexname,pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) index_size "
           "from pg_indexes order by pg_relation_size(schemaname||'.'||indexname) desc limit 50;")
    ret, indexinfo = run_psql_simple(sql)
    if ret:
        error("Query index size error\n")
        return
    info("---Index Size top 50\n")
    info_notimestr(f"{indexinfo}\n\n")


def chk_catalog() -> None:
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
    env["PGOPTIONS"] = f"-c {gp_session_role_name}=utility"
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, env=env
    )
    if result.returncode:
        print("pg_namespace master size error!")
        return
    tmp_result = result.stdout.strip()
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
    result = subprocess.run(
        ["psql", "-A", "-X", "-q", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, env=env
    )
    if result.returncode:
        print("pg_namespace realsize error!")
        return
    pg_namespace_realsize = int(result.stdout.strip())
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
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, env=env
    )
    if result.returncode:
        error("pg_class master size error! \n")
        return
    tmp_result = result.stdout.strip()
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
    result = subprocess.run(
        ["psql", "-A", "-X", "-q", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, env=env
    )
    if result.returncode:
        print("pg_class realsize error!")
        return
    pg_class_realsize = int(result.stdout.strip())
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
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, env=env
    )
    if result.returncode:
        error("pg_attribute master size error! \n")
        return
    tmp_result = result.stdout.strip()
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
    result = subprocess.run(
        ["psql", "-A", "-X", "-q", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, env=env
    )
    if result.returncode:
        print("pg_attribute realsize error!")
        return
    pg_attribute_realsize = int(result.stdout.strip())
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
    info_notimestr(f"pg_tables count:               {table_count}\n")
    info_notimestr(f"pg_views count:                {view_count}\n")
    info_notimestr(f"pg_namespace count:            {pg_namespace_count}\n")
    info_notimestr(f"pg_namespace size:             {pg_namespace_size}\n")
    info_notimestr(f"pg_namespace size in master:   {pg_namespace_master}\n")
    info_notimestr(f"pg_namespace size in gpseg0:   {pg_namespace_gpseg0}\n")
    info_notimestr(f"pg_namespace bloat in master:  {pg_namespace_master_bloat}\n")
    info_notimestr(f"pg_class count:                {pg_class_count}\n")
    info_notimestr(f"pg_class size:                 {pg_class_size}\n")
    info_notimestr(f"pg_class size in master:       {pg_class_master}\n")
    info_notimestr(f"pg_class size in gpseg0:       {pg_class_gpseg0}\n")
    info_notimestr(f"pg_class bloat in master:      {pg_class_master_bloat}\n")
    info_notimestr(f"pg_attribute count:            {pg_attribute_count}\n")
    info_notimestr(f"pg_attribute size:             {pg_attribute_size}\n")
    info_notimestr(f"pg_attribute size in master:   {pg_attribute_master}\n")
    info_notimestr(f"pg_attribute size in gpseg0:   {pg_attribute_gpseg0}\n")
    info_notimestr(f"pg_attribute bloat in master:  {pg_attribute_master_bloat}\n")
    info_notimestr(f"partition count:               {partition_count}\n")
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
    info_notimestr(f"{tabletype}\n")

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
    info_notimestr(f"{tabletype}\n")

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
    info_notimestr(f"{stat_ops}\n")


def chk_partition_info() -> None:
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
        info_notimestr(f"{subpart}\n")
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
        info_notimestr(f"{subpart}\n")

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
        info_notimestr(f"{part_schema}\n")


def _skew_worker(schema: str, h: str, p: str, u: str, d: str) -> None:
    """Child process worker for skew check."""
    sql = f"copy (select * from skewcheck_func('{schema}')) to '/tmp/tmpskew.{schema}.dat';"
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        capture_output=True, text=True
    )
    if result.returncode:
        sys.exit(-1)
    sql = f"copy check_skew_result from '/tmp/tmpskew.{schema}.dat';"
    subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        capture_output=True, text=True
    )


def skewcheck() -> None:
    print(f"---Begin to check skew, jobs [{concurrency}]")

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
    processes: list[Process] = []

    for icalc in range(itotal):
        while len([p for p in processes if p.is_alive()]) >= concurrency:
            time.sleep(1)

        p = Process(target=_skew_worker, args=(schema_list[icalc], hostname, port, username, database))
        p.start()
        processes.append(p)
        active = len([pp for pp in processes if pp.is_alive()])
        finished = len([pp for pp in processes if not pp.is_alive()])
        print(f"Child process count [{active}], finish count[{finished}/{itotal}]")

    for p in processes:
        p.join()

    print(f"Child process count [0], finish count[{itotal}/{itotal}]")

    sql = "select * from check_skew_result order by tablename,skew desc;"
    ret, skewresult = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query skew check result error! \n")
        return
    info("---Skew check\n")
    info_notimestr(f"\n{skewresult}\n")


def _bloat_worker(schema: str, h: str, p: str, u: str, d: str) -> None:
    """Child process worker for AO bloat check."""
    sql = f"copy (select schemaname||'.'||tablename,'ao',bloat from AOtable_bloatcheck('{schema}') where bloat>1.9) to '/tmp/tmpaobloat.{schema}.dat';"
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        capture_output=True, text=True
    )
    if result.returncode:
        sys.exit(-1)
    sql = f"copy bloat_skew_result from '/tmp/tmpaobloat.{schema}.dat';"
    subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        capture_output=True, text=True
    )


def bloatcheck() -> None:
    print(f"---Begin to check bloat, jobs [{concurrency}]")

    sql = ("drop table if exists bloat_skew_result; "
           "create table bloat_skew_result("
           "tablename text, relstorage varchar(10), bloat numeric(18,2)"
           ") distributed randomly;")
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True
    )
    if result.returncode:
        error("recreate bloat_skew_result error! \n")
        return

    if gpver == "gp7" or "cbdb" in gpver:
        pg_class_sql = "insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relam=2;"
    else:
        pg_class_sql = "insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';"

    # Heap table bloat check
    sql = (f"drop table if exists pg_stats_bloat_chk; "
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
           f"{pg_class_sql} "
           f"insert into pg_namespace_bloat_chk "
           f"select oid,nspname,nspowner from pg_namespace where nspname in {schema_str}; "
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
           ") AS blochk where wastedsize>104857600 and bloat>2;")

    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True
    )
    if result.returncode:
        error(f"Heap table bloat check error! \n{result.stdout}\n")
        return

    # AO table bloat check with parallel workers
    itotal = len(schema_list)
    processes: list[Process] = []

    for icalc in range(itotal):
        while len([p for p in processes if p.is_alive()]) >= concurrency:
            time.sleep(1)

        p = Process(target=_bloat_worker, args=(schema_list[icalc], hostname, port, username, database))
        p.start()
        processes.append(p)
        active = len([pp for pp in processes if pp.is_alive()])
        finished = len([pp for pp in processes if not pp.is_alive()])
        print(f"Child process count [{active}], finish count[{finished}/{itotal}]")

    for p in processes:
        p.join()

    print(f"Child process count [0], finish count[{itotal}/{itotal}]")

    sql = "select * from bloat_skew_result order by relstorage,bloat desc;"
    ret, bloatresult = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query bloat check result error! \n")
        return
    info("---Bloat check\n")
    info_notimestr(f"\n{bloatresult}\n")

    # Generate bloat fix script
    sql = "select count(*) from bloat_skew_result;"
    ret, bloatcount_str = run_psql_simple(sql)
    if ret:
        error("Query bloat table count error! \n")
        return
    bloatcount = int(bloatcount_str.strip())
    logday = get_current_date()
    if bloatcount > 0:
        sql = (f"copy (select 'alter table '||tablename||' set with (reorganize=true); analyze '||tablename||';' from bloat_skew_result) "
               f"to '{LOG_DIR}/fix_ao_table_script_{database}_{logday}.sql';")
        ret, _ = run_psql_simple(sql)
        if ret:
            error("Unload bloat table fix script error! \n")
            return
        info_notimestr(f"\nPlease check fix script: {LOG_DIR}/fix_ao_table_script_{database}_{logday}.sql\n")


def _defpart_worker(tablename: str, h: str, p: str, u: str, d: str) -> None:
    """Child process worker for default partition count."""
    sql = f"insert into def_partition_count_result select '{tablename}',count(*) from {tablename};"
    subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql, "-h", h, "-p", p, "-U", u, "-d", d],
        capture_output=True, text=True
    )


def def_partition() -> None:
    print(f"---Begin to check default partition, jobs [{concurrency}]")

    if gpver == "gp7" or "cbdb" in gpver:
        sql = (f"select c.nspname||'.'||b.relname from pg_partitioned_table a,pg_class b,pg_namespace c "
               f"where a.partdefid=b.oid and b.relnamespace=c.oid and b.relkind='r' and a.partdefid>0 and c.nspname in {schema_str};")
    else:
        sql = f"select partitionschemaname||'.'||partitiontablename from pg_partitions where partitionisdefault=true and partitionschemaname in {schema_str};"

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

    processes: list[Process] = []

    for icalc in range(itotal):
        while len([p for p in processes if p.is_alive()]) >= concurrency:
            time.sleep(1)

        p = Process(target=_defpart_worker, args=(defpart_list[icalc], hostname, port, username, database))
        p.start()
        processes.append(p)
        active = len([pp for pp in processes if pp.is_alive()])
        finished = len([pp for pp in processes if not pp.is_alive()])
        print(f"Child process count [{active}], finish count[{finished}/{itotal}]")

    for p in processes:
        p.join()

    print(f"Child process count [0], finish count[{itotal}/{itotal}]")

    sql = "select * from def_partition_count_result where row_count>0 order by row_count desc;"
    ret, defpartresult = run_psql([
        "psql", "-X", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Query default partition count result error! \n")
        return
    info("---Default partition check\n")
    info_notimestr(f"\n{defpartresult}\n")


def chk_os_param() -> None:
    print("---Check OS parameter")

    checks = [
        ('cat /etc/sysctl.conf |grep -vE \'^\\s*#|^\\s*$\'', "Check /etc/sysctl.conf", "Gpssh check sysctl.conf error\n", False),
        ('ulimit -a', "Check ulimit", "Gpssh check ulimit error\n", True),
        ('mount |grep xfs', "Check mount info", "Gpssh check mount info\n", True),
        ('cat /sys/kernel/mm/transparent_hugepage/enabled', "Check hugepage", "Gpssh check hugepage \n", True),
        ('date', "Check system clock", "Gpssh check system clock \n", True),
    ]

    for cmd, label, err_msg, do_return in checks:
        result = subprocess.run(
            f'gpssh -d 0 -f /tmp/tmpallhosts "{cmd}"',
            shell=True, capture_output=True, text=True
        )
        if result.returncode:
            error(err_msg)
            if do_return:
                return
        info(f"---{label} ...\n")
        info_notimestr(f"{result.stdout}\n\n")


def chk_gpdb_param() -> None:
    print("---Check GPDB parameter")

    if gpver == "gp7" or "cbdb" in gpver:
        master_dir = os.environ.get("COORDINATOR_DATA_DIRECTORY", "")
    else:
        master_dir = os.environ.get("MASTER_DATA_DIRECTORY", "")

    result = subprocess.run(
        f"cat {master_dir}/postgresql.conf | grep -vE '^\\s*#|^\\s*$'",
        shell=True, capture_output=True, text=True
    )
    if result.returncode:
        error("Check postgresql.conf error\n")
        return
    info("---Check setting in postgresql.conf ...\n")
    info_notimestr(f"{result.stdout}\n\n")

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
    info_notimestr(f"{param_info}\n")

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
    info_notimestr(f"{param_info}\n")


def main() -> None:
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
        print(f"Input error: \nPlease show help: python3 {cmd_name} --help")
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
    LOG_DIR = args.log_dir if args.log_dir else f"{home_dir}/gpAdminLogs"

    print(f"LOG Directory: {LOG_DIR}")

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
    info_notimestr(f"Hostname: {hostname}\nPort: {port}\nUsername: {username}\nConcurrency: {concurrency}\nLogDIR: {LOG_DIR}\n")
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
        print(f"------Begin to check database: {database}")
        info("-----------------------------------------------------\n")
        info(f"------Begin to check database: {database}\n")
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
