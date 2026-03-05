#!/usr/bin/env python
from __future__ import print_function
"""GPDB analyze for daily use.

Command line: python analyze_for_daily.py dbname schemaname concurrency
If schemaname=ALL, all schema will be analyzed!
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import Process

_DEVNULL = open(os.devnull, 'w')

EXCLUDE_SCHEMA = """
 'gp_toolkit'
,'ngpaatmpdata'
,'pg_toast'
,'pg_bitmapindex'
,'pg_catalog'
,'public'
,'information_schema'
,'gpexpand'
,'pg_aoseg'
,'oracompat'
,'monitor_old'
,'tmp_gpexport_copy'
,'stage'
,'tmp_job'
,'tmp'
,'monitor'
,'monitor_old'
,'workfile'
,'session_state'
"""


def set_env(database, username):
    os.environ["PGHOST"] = "localhost"
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = ""
    return 0


def get_curr_datetime():
    """Return datetime string for yesterday (matches Perl: time()-86400)."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d%H%M%S")


def run_psql(sql, quiet=False):
    """Run psql command and return (returncode, output)."""
    cmd = ["psql", "-A", "-X", "-t"]
    if quiet:
        cmd.append("-q")
    cmd.extend(["-c", sql])
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    return proc.returncode, out


def run_psql_stderr_suppressed(sql, quiet=False):
    """Run psql with stderr suppressed (2>/dev/null)."""
    cmd = ["psql", "-A", "-X", "-t"]
    if quiet:
        cmd.append("-q")
    cmd.extend(["-c", sql])
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=_DEVNULL)
    out, _ = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    return proc.returncode, out


def get_schema(inputschema):
    if inputschema == "ALL":
        sql = (
            " select string_agg(''''||nspname||'''',',' ) from pg_namespace"
            " where nspname not like 'pg%' and nspname not like 'gp%' and"
            " nspname not in ({});".format(EXCLUDE_SCHEMA)
        )
        ret, tmpsss = run_psql_stderr_suppressed(sql)
        if ret >> 8 if ret > 255 else ret:
            print("psql get all schema error")
            sys.exit(1)
        tmpsss = tmpsss.strip()
        curr_schema = "({})".format(tmpsss)
    else:
        tmpsss = inputschema.replace(",", "','")
        curr_schema = "('{}')".format(tmpsss)

    print("analyze schema [{}]".format(curr_schema))
    return curr_schema


def get_tablelist(curr_schema, currdatetime):
    target_tablelist = []

    tmp_schemastr = curr_schema.replace("'", "''")
    print("tmp_schemastr[{}]".format(tmp_schemastr))

    # Heap table list
    sql = (
        " select 'analyze '||aa.nspname||'.'||bb.relname||';'"
        " from pg_namespace aa inner join pg_class bb on aa.oid=bb.relnamespace"
        " left join pg_stat_last_operation o on bb.oid=o.objid and o.staactionname='ANALYZE'"
        " where aa.nspname in {} and bb.relkind='r' and bb.relstorage='h' and bb.relhassubclass=false"
        " and ((o.statime is null) or ((o.statime is not null) and (now() - o.statime > interval '3 day')));".format(curr_schema)
    )
    ret, output = run_psql(sql)
    if ret:
        print("Get heap table list error ={}=".format(sql))
        return -1, []
    target_tablelist.extend(output.splitlines())

    # AO table list
    sql = (
        " drop table if exists analyze_target_list_{currdatetime};"
        " create table analyze_target_list_{currdatetime} (like check_ao_state);"
        ""
        " insert into analyze_target_list_{currdatetime}"
        " select *,current_timestamp from get_AOtable_state_list('{tmp_schemastr}') a;"
        ""
        " create temp table ao_analyze_stat_temp ("
        "   reloid bigint,"
        "   schemaname text,"
        "   tablename text,"
        "   statime timestamp without time zone"
        " ) distributed by (reloid);"
        ""
        " insert into ao_analyze_stat_temp"
        " select objid,schemaname,objname,statime from pg_stat_operations op"
        " inner join ("
        "   select reloid,last_checktime,row_number() over(partition by reloid order by last_checktime desc) rn"
        "   from check_ao_state"
        " ) aost"
        " on op.objid=aost.reloid"
        " where op.actionname='ANALYZE' and aost.rn=1 and op.statime>=aost.last_checktime;"
        ""
        " select 'analyze '||schemaname||'.'||tablename||';' from"
        " ("
        "   select a.reloid,a.schemaname,a.tablename"
        "   from check_ao_state a,analyze_target_list_{currdatetime} b"
        "   where a.reloid=b.reloid and a.modcount<>b.modcount"
        "   union all"
        "   select b.reloid,b.schemaname,b.tablename"
        "   from analyze_target_list_{currdatetime} b"
        "   where b.reloid not in (select reloid from check_ao_state)"
        " ) t1"
        " where t1.reloid not in (select reloid from ao_analyze_stat_temp);".format(
            currdatetime=currdatetime, tmp_schemastr=tmp_schemastr
        )
    )
    print('psql -A -X -t -q -c "{}" '.format(sql))
    ret, output = run_psql_stderr_suppressed(sql, quiet=True)
    if ret:
        print("Get AO table list error ={}=".format(sql))
        return -1, []
    target_tablelist.extend(output.splitlines())

    return 0, target_tablelist


def run_after_analyze(curr_schema, currdatetime):
    tmp_schemastr = curr_schema.replace("'", "''")
    print("tmp_schemastr[{}]".format(tmp_schemastr))

    sql = (
        " delete from check_ao_state a"
        " using analyze_target_list_{currdatetime} b where a.reloid=b.reloid;"
        " delete from check_ao_state a"
        " where reloid not in (select oid from pg_class);"
        ""
        " insert into check_ao_state"
        " select reloid,schemaname,tablename,modcount,current_timestamp from analyze_target_list_{currdatetime} a;"
        ""
        " drop table if exists analyze_target_list_{currdatetime};".format(
            currdatetime=currdatetime
        )
    )
    print('psql -A -X -t -c "{}" '.format(sql))
    ret, _ = run_psql(sql)
    if ret:
        print("psql refresh AO table state error ={}=".format(sql))
        return -1

    sql = " vacuum analyze check_ao_state; "
    ret, _ = run_psql(sql)
    if ret:
        print("vacuum analyze check_ao_state error")
        return -1

    return 0


def analyze_worker(sql):
    """Child process: run a single ANALYZE command via psql."""
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("Analyze error: {}\n{}{}".format(sql, out, err))


def main():
    if len(sys.argv) != 4:
        print(
            "Argument number Error\nExample:\n"
            "python {} dbname schemaname concurrency\n"
            "If schemaname=ALL, all schema will be analyzed!".format(sys.argv[0])
        )
        sys.exit(1)

    database = sys.argv[1]
    inputschema = sys.argv[2]
    concurrency = int(sys.argv[3])

    proc = subprocess.Popen(
        ["whoami"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, _ = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    username = out.strip()

    currdatetime = get_curr_datetime()

    set_env(database, username)
    analyze_schema = get_schema(inputschema)
    print(analyze_schema)

    ret, target_tablelist = get_tablelist(analyze_schema, currdatetime)
    if ret:
        print("Get table list for analyze error!")
        return -1

    itotal = len(target_tablelist)
    print("Total count [{}]".format(itotal))

    num_finish = 0
    active_procs = []

    for icalc in range(itotal):
        sql = target_tablelist[icalc].strip()
        print("[SQL]=[{}]".format(sql))

        proc = Process(target=analyze_worker, args=(sql,))
        proc.start()
        active_procs.append(proc)

        if num_finish % 10 == 0:
            print(
                "Child process count [{}], "
                "finish count[{}/{}]".format(len(active_procs), num_finish, itotal)
            )

        # Wait until active processes < concurrency
        while len(active_procs) >= concurrency:
            time.sleep(1)
            still_active = []
            for p in active_procs:
                if p.is_alive():
                    still_active.append(p)
                else:
                    p.join()
                    num_finish += 1
            active_procs = still_active

    # Wait for all child processes
    print("waiting for all child finished!")
    for p in active_procs:
        p.join()
        num_finish += 1

    run_after_analyze(analyze_schema, currdatetime)

    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
