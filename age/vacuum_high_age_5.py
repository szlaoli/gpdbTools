#!/usr/bin/env python3
"""GPDB vacuum high age table - for GP5.

Command line: python3 vacuum_high_age_5.py dbname duration(hours) log_dir
Sample: python3 vacuum_high_age_5.py gsdc 2 /home/gpadmin/gpAdminLogs/
"""

import os
import subprocess
import sys
import time
from datetime import datetime
from multiprocessing import Process
from typing import IO

HOSTNAME = "localhost"
PORT = "5432"
USERNAME = "gpadmin"
PASSWORD = ""
CONCURRENCY = 3
AGE_LEVEL = "300000000"

fh_log: IO[str] | None = None


def set_env(database: str) -> int:
    os.environ["PGHOST"] = HOSTNAME
    os.environ["PGPORT"] = PORT
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = USERNAME
    os.environ["PGPASSWORD"] = PASSWORD

    subprocess.run(
        "source /usr/local/greenplum-db/greenplum_path.sh",
        shell=True,
        executable="/bin/bash",
    )
    return 0


def get_current_date() -> str:
    return datetime.now().strftime("%Y%m%d")


def show_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_psql(sql: str) -> tuple[int, str]:
    """Run psql command and return (returncode, output)."""
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql],
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout


def get_tablelist() -> tuple[int, list[str]]:
    global fh_log
    target_tablelist: list[str] = []

    # Prepare high age tableinfo (rootpartition / none partition)
    sql = f"""
    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relhassubclass=true
    distributed randomly;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relhassubclass=true;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relhassubclass=false and relname not like '%_1_prt_%';

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relhassubclass=false and relname not like '%_1_prt_%';
    """
    fh_log.write(f"[INFO]psql -A -X -t -c [{sql}]\n")
    ret, _ = run_psql(sql)
    if ret:
        fh_log.write(f"[ERROR]Get partition tablelist error [{sql}]\n")
        return -1, []

    # Get high age table list
    sql = """
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 8000
    """
    fh_log.write(f"[INFO]psql -A -X -t -c [{sql}]\n")
    ret, output = run_psql(sql)
    if ret:
        fh_log.write(f"[ERROR]Get partition tablelist error [{sql}]\n")
        return -1, []
    target_tablelist.extend(output.splitlines())

    # Prepare high age tableinfo (sub partition)
    sql = f"""
    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relhassubclass=false and relname like '%_1_prt_%'
    distributed randomly;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relhassubclass=false and relname like '%_1_prt_%';
    """
    fh_log.write(f"[INFO]psql -A -X -t -c [{sql}]\n")
    ret, _ = run_psql(sql)
    if ret:
        fh_log.write(f"[ERROR]Get partition tablelist error [{sql}]\n")
        return -1, []

    # Get high age table list
    sql = """
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 8000
    """
    fh_log.write(f"[INFO]psql -A -X -t -c [{sql}]\n")
    ret, output = run_psql(sql)
    if ret:
        fh_log.write(f"[ERROR]Get partition tablelist error [{sql}]\n")
        return -1, []
    target_tablelist.extend(output.splitlines())

    # Prepare high age tableinfo (pg_aoseg)
    sql = f"""
    drop table if exists tmp_pg_aoseg;
    create table tmp_pg_aoseg as
    select oid as relaosegoid,relname as relaosegname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid,relkind from gp_dist_random('pg_class')
    where age(relfrozenxid)>{AGE_LEVEL} and age(relfrozenxid)<>2147483647 and relnamespace=6104
    distributed randomly;

    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select c.oid as reloid,c.relname,a.age_int,a.segid
    from tmp_pg_aoseg a,pg_appendonly b,pg_class c
    where a.relaosegoid=b.segrelid and b.relid=c.oid and a.relkind='o';

    insert into tmp_class_age
    select c.oid as reloid,c.relname,a.age_int,a.segid
    from tmp_pg_aoseg a,pg_appendonly b,pg_class c
    where a.relaosegoid=b.visimaprelid and b.relid=c.oid and a.relkind='m';

    insert into tmp_class_age
    select c.oid as reloid,c.relname,a.age_int,a.segid
    from tmp_pg_aoseg a,pg_appendonly b,pg_class c
    where a.relaosegoid=b.blkdirrelid and b.relid=c.oid and a.relkind='b';
    """
    fh_log.write(f"[INFO]psql -A -X -t -c [{sql}]\n")
    ret, _ = run_psql(sql)
    if ret:
        fh_log.write(f"[ERROR]Get partition tablelist error [{sql}]\n")
        return -1, []

    # Get high age table list
    sql = """
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 3000
    """
    fh_log.write(f"[INFO]psql -A -X -t -c [{sql}]\n")
    ret, output = run_psql(sql)
    if ret:
        fh_log.write(f"[ERROR]Get partition tablelist error [{sql}]\n")
        return -1, []
    target_tablelist.extend(output.splitlines())

    return 0, target_tablelist


def vacuum_worker(sql: str) -> None:
    """Child process: run a single VACUUM FREEZE command via psql."""
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(f"[ERROR]VACUUM error: [{sql}]\nerrmsg: [{result.stdout}]")


def main() -> int:
    global fh_log

    if len(sys.argv) != 4:
        print(
            f"Argument number Error\nExample:\n"
            f"python3 {sys.argv[0]} dbname duration(hours) log_dir"
        )
        sys.exit(1)

    database = sys.argv[1]
    duration = float(sys.argv[2])
    log_dir = sys.argv[3]

    set_env(database)

    logday = get_current_date()
    log_path = os.path.join(log_dir, f"vacuum_high_age_{logday}.log")
    try:
        fh_log = open(log_path, "a")
    except OSError:
        print(f"[ERROR]:Could not open logfile {log_path}")
        sys.exit(1)

    fh_log.write(f"[INFO]:Start time:{show_time()}\n")
    starttime = time.time()

    ret, target_tablelist = get_tablelist()
    if ret:
        fh_log.write("[ERROR]Get high age table list error!\n")
        fh_log.write(f"[INFO]:Finish time:{show_time()}\n")
        fh_log.close()
        return -1

    itotal = len(target_tablelist)
    fh_log.write(f"[INFO]Total count [{itotal}]\n")

    num_finish = 0
    active_procs: list[Process] = []

    for icalc in range(itotal):
        nowtime = time.time()
        t_interval = nowtime - starttime
        fh_log.write(f"[INFO]t_interval:[{t_interval}]\n")
        if t_interval > duration * 3600:
            fh_log.write("[INFO]Time is up\n")
            break

        sql = target_tablelist[icalc].strip()
        fh_log.write(f"[INFO][SQL]=[{sql}]\n")
        fh_log.flush()

        proc = Process(target=vacuum_worker, args=(sql,))
        proc.start()
        active_procs.append(proc)

        if num_finish % 10 == 0:
            print(
                f"Child process count [{len(active_procs)}], "
                f"finish count[{num_finish}/{itotal}]"
            )

        # Wait until active processes < concurrency
        while len(active_procs) >= CONCURRENCY:
            time.sleep(1)
            still_active: list[Process] = []
            for p in active_procs:
                if p.is_alive():
                    still_active.append(p)
                else:
                    p.join()
                    num_finish += 1
            active_procs = still_active

    # Wait for remaining child processes
    if active_procs:
        unfinish = len(active_procs)
        fh_log.write(
            f"[INFO]:Waiting for {unfinish} unfinished child processes\n"
        )

    for p in active_procs:
        p.join()
        num_finish += 1

    fh_log.write(f"[INFO]:Finish time:{show_time()}\n")
    fh_log.close()
    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
