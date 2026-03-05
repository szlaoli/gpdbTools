#!/usr/bin/env python
"""GPDB vacuum high age table.

Command line: python vacuum_high_age.py dbname duration(hours) log_dir
Sample: python vacuum_high_age.py gsdc 2 /home/gpadmin/gpAdminLogs/
"""

from __future__ import print_function

import os
import subprocess
import sys
import time
from datetime import datetime
from multiprocessing import Process, Value

HOSTNAME = "localhost"
PORT = "5432"
USERNAME = "gpadmin"
PASSWORD = ""
CONCURRENCY = 3
AGE_LEVEL = "300000000"

fh_log = None


def set_env(database):
    os.environ["PGHOST"] = HOSTNAME
    os.environ["PGPORT"] = PORT
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = USERNAME
    os.environ["PGPASSWORD"] = PASSWORD

    proc = subprocess.Popen(
        "source /usr/local/greenplum-db/greenplum_path.sh",
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    proc.communicate()
    return 0


def get_current_datetime():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_current_date():
    return datetime.now().strftime("%Y%m%d")


def show_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_psql(sql):
    """Run psql command and return (returncode, output)."""
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return proc.returncode, out


def get_tablelist():
    global fh_log
    target_tablelist = []

    # Prepare high age tableinfo
    sql = """
    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{age_level} and relhassubclass=true
    distributed randomly;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{age_level} and relhassubclass=true;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{age_level} and relhassubclass=false and relname not like '%_1_prt_%';

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{age_level} and relhassubclass=false and relname not like '%_1_prt_%';
    """.format(age_level=AGE_LEVEL)
    fh_log.write("[INFO]psql -A -X -t -c [{}]\n".format(sql))
    ret, _ = run_psql(sql)
    if ret:
        fh_log.write("[ERROR]Get partition tablelist error [{}]\n".format(sql))
        return -1, []

    # Get high age table list
    sql = """
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 8000
    """
    fh_log.write("[INFO]psql -A -X -t -c [{}]\n".format(sql))
    ret, output = run_psql(sql)
    if ret:
        fh_log.write("[ERROR]Get partition tablelist error [{}]\n".format(sql))
        return -1, []
    target_tablelist.extend(output.splitlines())

    # Prepare high age tableinfo (partitions)
    sql = """
    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{age_level} and relhassubclass=false and relname like '%_1_prt_%'
    distributed randomly;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>{age_level} and relhassubclass=false and relname like '%_1_prt_%';
    """.format(age_level=AGE_LEVEL)
    fh_log.write("[INFO]psql -A -X -t -c [{}]\n".format(sql))
    ret, _ = run_psql(sql)
    if ret:
        fh_log.write("[ERROR]Get partition tablelist error [{}]\n".format(sql))
        return -1, []

    # Get high age table list
    sql = """
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 8000
    """
    fh_log.write("[INFO]psql -A -X -t -c [{}]\n".format(sql))
    ret, output = run_psql(sql)
    if ret:
        fh_log.write("[ERROR]Get partition tablelist error [{}]\n".format(sql))
        return -1, []
    target_tablelist.extend(output.splitlines())

    return 0, target_tablelist


def vacuum_worker(sql):
    """Child process: run a single VACUUM FREEZE command via psql."""
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        # Write to stdout since log file handle is not shared across processes
        print("[ERROR]VACUUM error: [{}]\nerrmsg: [{}]".format(sql, out))


def main():
    global fh_log

    if len(sys.argv) != 4:
        print(
            "Argument number Error\nExample:\n"
            "python {} dbname duration(hours) log_dir".format(sys.argv[0])
        )
        sys.exit(1)

    database = sys.argv[1]
    duration = float(sys.argv[2])
    log_dir = sys.argv[3]

    set_env(database)

    logday = get_current_date()
    log_path = os.path.join(log_dir, "vacuum_high_age_{}.log".format(logday))
    try:
        fh_log = open(log_path, "a")
    except OSError:
        print("[ERROR]:Could not open logfile {}".format(log_path))
        sys.exit(1)

    fh_log.write("[INFO]:Start time:{}\n".format(show_time()))
    starttime = time.time()

    ret, target_tablelist = get_tablelist()
    if ret:
        fh_log.write("[ERROR]Get high age table list error!\n")
        fh_log.write("[INFO]:Finish time:{}\n".format(show_time()))
        fh_log.close()
        return -1

    itotal = len(target_tablelist)
    fh_log.write("[INFO]Total count [{}]\n".format(itotal))

    num_finish = 0
    active_procs = []

    for icalc in range(itotal):
        nowtime = time.time()
        t_interval = nowtime - starttime
        fh_log.write("[INFO]t_interval:[{}]\n".format(t_interval))
        if t_interval > duration * 3600:
            fh_log.write("[INFO]Time is up\n")
            break

        sql = target_tablelist[icalc].strip()
        fh_log.write("[INFO][SQL]=[{}]\n".format(sql))
        fh_log.flush()

        proc = Process(target=vacuum_worker, args=(sql,))
        proc.start()
        active_procs.append(proc)

        if num_finish % 10 == 0:
            print(
                "Child process count [{}], "
                "finish count[{}/{}]".format(len(active_procs), num_finish, itotal)
            )

        # Wait until active processes < concurrency
        while len(active_procs) >= CONCURRENCY:
            time.sleep(1)
            still_active = []
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
            "[INFO]:Waiting for {} unfinished child processes\n".format(unfinish)
        )

    for p in active_procs:
        p.join()
        num_finish += 1

    fh_log.write("[INFO]:Finish time:{}\n".format(show_time()))
    fh_log.close()
    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
