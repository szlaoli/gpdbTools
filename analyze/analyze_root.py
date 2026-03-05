#!/usr/bin/env python3
"""GPDB analyze root partition tables.

Command line: python3 analyze_root.py dbname schema concurrency
"""

import os
import subprocess
import sys
import time
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


def run_psql(sql: str) -> tuple[int, str]:
    """Run psql command and return (returncode, output)."""
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql],
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout


def run_psql_stderr_suppressed(sql: str) -> tuple[int, str]:
    """Run psql with stderr suppressed."""
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql],
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


def get_tablelist(curr_schema: str) -> tuple[int, list[str]]:
    # root partition
    sql = (
        f" select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'"
        f" from pg_namespace aa,pg_class bb"
        f" where aa.oid=bb.relnamespace and aa.nspname in {curr_schema}"
        f" and bb.relkind='r' and bb.relstorage!='x' and bb.relhassubclass=true; "
    )
    print(f'psql -A -X -t -c "{sql}" ')
    ret, output = run_psql(sql)
    if ret >> 8 if ret > 255 else ret:
        print("Get rootpartition error ")
        return -1, []

    return 0, output.splitlines()


def analyze_worker(sql: str) -> None:
    """Child process: run a single ANALYZE command via psql."""
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(f"Analyze error: {result.stdout}{result.stderr}")


def main() -> int:
    if len(sys.argv) != 4:
        print(
            f"Argument number Error\nExample:\n"
            f"python3 {sys.argv[0]} dbname schema concurrency"
        )
        sys.exit(1)

    database = sys.argv[1]
    inputschema = sys.argv[2]
    concurrency = int(sys.argv[3])

    username = subprocess.run(
        ["whoami"], capture_output=True, text=True
    ).stdout.strip()

    set_env(database, username)

    analyze_schema = get_schema(inputschema)
    print(analyze_schema)

    ret, target_tablelist = get_tablelist(analyze_schema)
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

    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
