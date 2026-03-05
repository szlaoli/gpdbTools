#!/usr/bin/env python
from __future__ import print_function
"""GPDB analyze root partition tables.

Command line: python analyze_root.py dbname schema concurrency
"""

import os
import subprocess
import sys
import time
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


def run_psql_stderr_suppressed(sql):
    """Run psql with stderr suppressed."""
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql],
        stdout=subprocess.PIPE, stderr=_DEVNULL,
    )
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


def get_tablelist(curr_schema):
    # root partition
    sql = (
        " select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'"
        " from pg_namespace aa,pg_class bb"
        " where aa.oid=bb.relnamespace and aa.nspname in {}"
        " and bb.relkind='r' and bb.relstorage!='x' and bb.relhassubclass=true; ".format(curr_schema)
    )
    print('psql -A -X -t -c "{}" '.format(sql))
    ret, output = run_psql(sql)
    if ret >> 8 if ret > 255 else ret:
        print("Get rootpartition error ")
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
    inputschema = sys.argv[2]
    concurrency = int(sys.argv[3])

    proc = subprocess.Popen(
        ["whoami"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, _ = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    username = out.strip()

    set_env(database, username)

    analyze_schema = get_schema(inputschema)
    print(analyze_schema)

    ret, target_tablelist = get_tablelist(analyze_schema)
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

    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
