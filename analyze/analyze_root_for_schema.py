#!/usr/bin/env python
from __future__ import print_function
"""GPDB analyze root partition tables for specific schema.

Command line: python analyze_root_for_schema.py dbname schema concurrency
"""

import os
import subprocess
import sys
import time
from multiprocessing import Process


def set_env(database):
    os.environ["PGHOST"] = "localhost"
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = "gpadmin"
    os.environ["PGPASSWORD"] = "gpadmin"
    return 0


def run_psql(sql):
    """Run psql command and return (returncode, output)."""
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    return proc.returncode, out


def get_tablelist(schemaname):
    # root partition
    if schemaname == "ALL":
        sql = (
            " select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'"
            " from pg_namespace aa,pg_class bb"
            " where aa.oid=bb.relnamespace and aa.nspname not like 'pg%' and aa.nspname not like 'gp%'"
            " and bb.relkind='r' and bb.relstorage!='x'"
            " and bb.relhassubclass=true; "
        )
        print('psql -A -X -t -c "{}" '.format(sql))
        ret, output = run_psql(sql)
        if ret >> 8 if ret > 255 else ret:
            print("psql ALL rootpartition error ")
            return -1, []
    else:
        tmpsss = schemaname.replace(",", "','")
        curr_schema = "('{}')".format(tmpsss)
        sql = (
            " select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'"
            " from pg_namespace aa,pg_class bb"
            " where aa.oid=bb.relnamespace and aa.nspname in {}"
            " and bb.relkind='r' and bb.relstorage!='x'"
            " and bb.relhassubclass=true; ".format(curr_schema)
        )
        print('psql -A -X -t -c "{}" '.format(sql))
        ret, output = run_psql(sql)
        if ret >> 8 if ret > 255 else ret:
            print("psql rootpartition error ")
            return -1, []

    return 0, output.splitlines()


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
        print("Analyze error: {}{}".format(out, err))


def main():
    if len(sys.argv) != 4:
        print(
            "Argument number Error\nExample:\n"
            "python {} dbname schema concurrency".format(sys.argv[0])
        )
        sys.exit(1)

    database = sys.argv[1]
    schemaname = sys.argv[2]
    concurrency = int(sys.argv[3])

    set_env(database)

    ret, target_tablelist = get_tablelist(schemaname)
    if ret:
        print("Get table list for analyze error!")
        sys.exit(1)

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
    for p in active_procs:
        p.join()
        num_finish += 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
