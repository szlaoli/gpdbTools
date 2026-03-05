#!/usr/bin/env python3
"""GPDB analyze for daily use.

Command line: python3 analyze_for_daily.py dbname schemaname concurrency
If schemaname=ALL, all schema will be analyzed!
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import Process

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


def set_env(database: str, username: str) -> int:
    os.environ["PGHOST"] = "localhost"
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = ""
    return 0


def get_curr_datetime() -> str:
    """Return datetime string for yesterday (matches Perl: time()-86400)."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d%H%M%S")


def run_psql(sql: str, quiet: bool = False) -> tuple[int, str]:
    """Run psql command and return (returncode, output)."""
    cmd = ["psql", "-A", "-X", "-t"]
    if quiet:
        cmd.append("-q")
    cmd.extend(["-c", sql])
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout


def run_psql_stderr_suppressed(sql: str, quiet: bool = False) -> tuple[int, str]:
    """Run psql with stderr suppressed (2>/dev/null)."""
    cmd = ["psql", "-A", "-X", "-t"]
    if quiet:
        cmd.append("-q")
    cmd.extend(["-c", sql])
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return result.returncode, result.stdout


def get_schema(inputschema: str) -> str:
    if inputschema == "ALL":
        sql = (
            f" select string_agg(''''||nspname||'''',',' ) from pg_namespace"
            f" where nspname not like 'pg%' and nspname not like 'gp%' and"
            f" nspname not in ({EXCLUDE_SCHEMA}); "
        )
        ret, tmpsss = run_psql_stderr_suppressed(sql)
        if ret >> 8 if ret > 255 else ret:
            print("psql get all schema error")
            sys.exit(1)
        tmpsss = tmpsss.strip()
        curr_schema = f"({tmpsss})"
    else:
        tmpsss = inputschema.replace(",", "','")
        curr_schema = f"('{tmpsss}')"

    print(f"analyze schema [{curr_schema}]")
    return curr_schema


def get_tablelist(
    curr_schema: str, currdatetime: str
) -> tuple[int, list[str]]:
    target_tablelist: list[str] = []

    tmp_schemastr = curr_schema.replace("'", "''")
    print(f"tmp_schemastr[{tmp_schemastr}]")

    # Heap table list
    sql = (
        f" select 'analyze '||aa.nspname||'.'||bb.relname||';'"
        f" from pg_namespace aa inner join pg_class bb on aa.oid=bb.relnamespace"
        f" left join pg_stat_last_operation o on bb.oid=o.objid and o.staactionname='ANALYZE'"
        f" where aa.nspname in {curr_schema} and bb.relkind='r' and bb.relstorage='h' and bb.relhassubclass=false"
        f" and ((o.statime is null) or ((o.statime is not null) and (now() - o.statime > interval '3 day')));"
    )
    ret, output = run_psql(sql)
    if ret:
        print(f"Get heap table list error ={sql}=")
        return -1, []
    target_tablelist.extend(output.splitlines())

    # AO table list
    sql = (
        f" drop table if exists analyze_target_list_{currdatetime};"
        f" create table analyze_target_list_{currdatetime} (like check_ao_state);"
        f""
        f" insert into analyze_target_list_{currdatetime}"
        f" select *,current_timestamp from get_AOtable_state_list('{tmp_schemastr}') a;"
        f""
        f" create temp table ao_analyze_stat_temp ("
        f"   reloid bigint,"
        f"   schemaname text,"
        f"   tablename text,"
        f"   statime timestamp without time zone"
        f" ) distributed by (reloid);"
        f""
        f" insert into ao_analyze_stat_temp"
        f" select objid,schemaname,objname,statime from pg_stat_operations op"
        f" inner join ("
        f"   select reloid,last_checktime,row_number() over(partition by reloid order by last_checktime desc) rn"
        f"   from check_ao_state"
        f" ) aost"
        f" on op.objid=aost.reloid"
        f" where op.actionname='ANALYZE' and aost.rn=1 and op.statime>=aost.last_checktime;"
        f""
        f" select 'analyze '||schemaname||'.'||tablename||';' from"
        f" ("
        f"   select a.reloid,a.schemaname,a.tablename"
        f"   from check_ao_state a,analyze_target_list_{currdatetime} b"
        f"   where a.reloid=b.reloid and a.modcount<>b.modcount"
        f"   union all"
        f"   select b.reloid,b.schemaname,b.tablename"
        f"   from analyze_target_list_{currdatetime} b"
        f"   where b.reloid not in (select reloid from check_ao_state)"
        f" ) t1"
        f" where t1.reloid not in (select reloid from ao_analyze_stat_temp);"
    )
    print(f'psql -A -X -t -q -c "{sql}" ')
    ret, output = run_psql_stderr_suppressed(sql, quiet=True)
    if ret:
        print(f"Get AO table list error ={sql}=")
        return -1, []
    target_tablelist.extend(output.splitlines())

    return 0, target_tablelist


def run_after_analyze(curr_schema: str, currdatetime: str) -> int:
    tmp_schemastr = curr_schema.replace("'", "''")
    print(f"tmp_schemastr[{tmp_schemastr}]")

    sql = (
        f" delete from check_ao_state a"
        f" using analyze_target_list_{currdatetime} b where a.reloid=b.reloid;"
        f" delete from check_ao_state a"
        f" where reloid not in (select oid from pg_class);"
        f""
        f" insert into check_ao_state"
        f" select reloid,schemaname,tablename,modcount,current_timestamp from analyze_target_list_{currdatetime} a;"
        f""
        f" drop table if exists analyze_target_list_{currdatetime};"
    )
    print(f'psql -A -X -t -c "{sql}" ')
    ret, _ = run_psql(sql)
    if ret:
        print(f"psql refresh AO table state error ={sql}=")
        return -1

    sql = " vacuum analyze check_ao_state; "
    ret, _ = run_psql(sql)
    if ret:
        print("vacuum analyze check_ao_state error")
        return -1

    return 0


def analyze_worker(sql: str) -> None:
    """Child process: run a single ANALYZE command via psql."""
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(f"Analyze error: {sql}\n{result.stdout}{result.stderr}")


def main() -> int:
    if len(sys.argv) != 4:
        print(
            f"Argument number Error\nExample:\n"
            f"python3 {sys.argv[0]} dbname schemaname concurrency\n"
            f"If schemaname=ALL, all schema will be analyzed!"
        )
        sys.exit(1)

    database = sys.argv[1]
    inputschema = sys.argv[2]
    concurrency = int(sys.argv[3])

    username = subprocess.run(
        ["whoami"], capture_output=True, text=True
    ).stdout.strip()

    currdatetime = get_curr_datetime()

    set_env(database, username)
    analyze_schema = get_schema(inputschema)
    print(analyze_schema)

    ret, target_tablelist = get_tablelist(analyze_schema, currdatetime)
    if ret:
        print("Get table list for analyze error!")
        return -1

    itotal = len(target_tablelist)
    print(f"Total count [{itotal}]")

    num_finish = 0
    active_procs: list[Process] = []

    for icalc in range(itotal):
        sql = target_tablelist[icalc].strip()
        print(f"[SQL]=[{sql}]")

        proc = Process(target=analyze_worker, args=(sql,))
        proc.start()
        active_procs.append(proc)

        if num_finish % 10 == 0:
            print(
                f"Child process count [{len(active_procs)}], "
                f"finish count[{num_finish}/{itotal}]"
            )

        # Wait until active processes < concurrency
        while len(active_procs) >= concurrency:
            time.sleep(1)
            still_active: list[Process] = []
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
