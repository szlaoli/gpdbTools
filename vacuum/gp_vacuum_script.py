#!/usr/bin/env python3
"""Greenplum vacuum script: bloat check and parallel vacuum with process deduplication."""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from io import TextIOWrapper
from typing import Optional

# Globals
hostname: str = "localhost"
port: str = "5432"
database: str = "postgres"
username: str = "gpadmin"
password: str = "gpadmin"

schema_list: list[str] = []
schema_str: str = ""
concurrency: int = 2

fh_log: Optional[TextIOWrapper] = None
cmd_name: str = ""

num_proc: int = 0
num_finish: int = 0
mainpid: int = os.getpid()


def handler(signum: int, frame: object) -> None:
    """Handle SIGCHLD: reap finished child processes."""
    global num_proc, num_finish

    c_pid = os.getpid()
    if c_pid == mainpid:
        if num_proc == 0:
            return
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid <= 0:
                    break
                num_proc -= 1
                num_finish += 1
            except ChildProcessError:
                break


signal.signal(signal.SIGCHLD, handler)


def get_cmd_name(inname: str) -> str:
    """Extract the basename from a full path."""
    return os.path.basename(inname)


def get_current_date() -> str:
    """Return current date as YYYYMMDD string."""
    return datetime.now().strftime("%Y%m%d")


def show_time() -> str:
    """Return current timestamp as YYYY-MM-DD HH:MM:SS string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_log() -> None:
    """Open the log file for appending."""
    global fh_log
    logday = get_current_date()
    log_dir = os.path.expanduser("~/gpAdminLogs")
    log_path = os.path.join(log_dir, f"{cmd_name}_{logday}.log")
    try:
        os.makedirs(log_dir, exist_ok=True)
        fh_log = open(log_path, "a")
    except OSError:
        print(f"[ERROR]:Could not open logfile {log_path}")
        sys.exit(-1)


def info(printmsg: str) -> int:
    """Write an INFO message to the log file."""
    if fh_log is not None:
        fh_log.write(f"[{show_time()} INFO] {printmsg}")
    return 0


def info_notimestr(printmsg: str) -> int:
    """Write a message to the log file without timestamp prefix."""
    if fh_log is not None:
        fh_log.write(printmsg)
    return 0


def error(printmsg: str) -> int:
    """Write an ERROR message to the log file."""
    if fh_log is not None:
        fh_log.write(f"[{show_time()} ERROR] {printmsg}")
    return 0


def close_log() -> int:
    """Close the log file."""
    if fh_log is not None:
        fh_log.close()
    return 0


def set_env() -> int:
    """Set PostgreSQL environment variables."""
    os.environ["PGHOST"] = hostname
    os.environ["PGPORT"] = port
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password
    return 0


def run_psql(sql: str, stderr_devnull: bool = False) -> subprocess.CompletedProcess[str]:
    """Run a psql command with standard connection parameters."""
    cmd = ["psql", "-A", "-X", "-t", "-c", sql,
           "-h", hostname, "-p", port, "-U", username, "-d", database]
    stderr_target = subprocess.DEVNULL if stderr_devnull else subprocess.PIPE
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=stderr_target,
                          text=True, check=False)


def check_process() -> int:
    """Check if another instance of this script is running."""
    result = subprocess.run(
        f"ps -ef | grep {cmd_name} | grep -v grep | grep -v '.log' | wc -l",
        shell=True, capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        error(f"Check {cmd_name} process error\n")
        return -1
    return int(result.stdout.strip())


def get_schema(
    is_all: bool,
    chk_schema: list[str],
    schema_file: str,
    exclude_schema: list[str],
    exclude_schema_file: str,
) -> None:
    """Populate schema_list and schema_str based on CLI options."""
    global schema_list, schema_str

    # --all
    if is_all:
        sql = "select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' order by 1;"
        result = run_psql(sql)
        if result.returncode != 0:
            error("Query all schema name error\n")
            sys.exit(1)
        schema_list = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]

    # --include-schema
    if chk_schema:
        schema_list.extend(chk_schema)

    # --include-schema-file
    if schema_file:
        if not os.path.exists(schema_file):
            error(f"Schema file {schema_file} do not exist!\n")
            sys.exit(1)
        try:
            with open(schema_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        schema_list.append(line)
        except OSError as e:
            print(f"{show_time()} open {schema_file} error: {e}")
            sys.exit(1)

    # --exclude-schema
    if exclude_schema:
        ex_str = "(" + ",".join(f"'{s}'" for s in exclude_schema) + ")"
        print(f"Exclude SCHEMA: {ex_str}")
        sql = (f"select nspname from pg_namespace where nspname not like 'pg%' "
               f"and nspname not like 'gp%' and nspname not in {ex_str} order by 1;")
        result = run_psql(sql)
        if result.returncode != 0:
            error("Query schema name exclude error\n")
            sys.exit(1)
        schema_list = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]

    # --exclude-schema-file
    if exclude_schema_file:
        if not os.path.exists(exclude_schema_file):
            error(f"Schema file {exclude_schema_file} do not exist!\n")
            sys.exit(1)
        exclude_list: list[str] = []
        try:
            with open(exclude_schema_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        exclude_list.append(line)
        except OSError as e:
            print(f"{show_time()} open {exclude_schema_file} error: {e}")
            sys.exit(1)

        ex_str = "(" + ",".join(f"'{s}'" for s in exclude_list) + ")"
        print(f"Exclude SCHEMA: {ex_str}")
        sql = (f"select nspname from pg_namespace where nspname not like 'pg%' "
               f"and nspname not like 'gp%' and nspname not in {ex_str} order by 1;")
        result = run_psql(sql)
        if result.returncode != 0:
            error("Query schema name exclude file error\n")
            sys.exit(1)
        schema_list = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]

    # Build schema_str
    schema_str = "(" + ",".join(f"'{s}'" for s in schema_list) + ")"
    print(f"SCHEMA: {schema_str}")


def bloatcheck() -> int:
    """Perform bloat check on heap and AO tables."""
    global num_proc, num_finish

    print(f"---Start bloat check, jobs [{concurrency}]")

    # Create result table
    sql = """drop table if exists bloat_skew_result;
             create table bloat_skew_result(
               tablename text,
               relstorage varchar(10),
               bloat numeric(18,2)
             ) distributed randomly;"""
    result = run_psql(sql)
    if result.returncode != 0:
        error("recreate bloat_skew_result error! \n")
        return -1

    # Heap table bloat check
    info("---Start heap table bloat check...\n")
    sql = f"""drop table if exists pg_stats_bloat_chk;
             create temp table pg_stats_bloat_chk
             (
               schemaname varchar(30),
               tablename varchar(80),
               attname varchar(100),
               null_frac float4,
               avg_width int4,
               n_distinct float4
             ) distributed by (tablename);

             drop table if exists pg_class_bloat_chk;
             create temp table pg_class_bloat_chk (like pg_class) distributed by (relname);

             drop table if exists pg_namespace_bloat_chk;
             create temp table pg_namespace_bloat_chk
             (
               oid_ss integer,
               nspname varchar(50),
               nspowner integer
             ) distributed by (oid_ss);

             insert into pg_stats_bloat_chk
             select schemaname,tablename,attname,null_frac,avg_width,n_distinct from pg_stats;

             insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';

             insert into pg_namespace_bloat_chk
             select oid,nspname,nspowner from pg_namespace where nspname in {schema_str};


             insert into bloat_skew_result
             SELECT schemaname||'.'||tablename,'h',bloat
             FROM (
               SELECT
                   current_database() as datname,
                   'table' as tabletype,
                   schemaname,
                   tablename,
                   reltuples::bigint AS tuples,
                   rowsize::float::bigint AS rowsize,
                   live_size_blocks*bs as total_size_tuples,
                   bs*relpages::bigint AS total_size_pages,
                   ROUND (
                       CASE
                           WHEN live_size_blocks = 0 AND relpages > 0 THEN 1000.0
                           ELSE sml.relpages/live_size_blocks::numeric
                       END,
                       1
                   ) AS bloat,
                   CASE
                       WHEN relpages <  live_size_blocks THEN 0::bigint
                       ELSE (bs*(relpages-live_size_blocks))::bigint
                   END AS wastedsize
               FROM (
                   SELECT
                       schemaname,
                       tablename,
                       cc.reltuples,
                       cc.relpages,
                       bs,
                       CEIL (
                           (cc.reltuples*( (datahdr + maxalign - (CASE WHEN datahdr%maxalign =  0 THEN maxalign ELSE datahdr%maxalign END)) + nullhdr2 + 4 ) )/(bs-20::float)
                       ) AS live_size_blocks,
                       ( (datahdr + maxalign - (CASE WHEN datahdr%maxalign =  0 THEN maxalign ELSE datahdr%maxalign END)) + nullhdr2 + 4 ) as rowsize
                   FROM (
                       SELECT
                           maxalign,
                           bs,
                           schemaname,
                           tablename,
                           (datawidth + (hdr + maxalign - (case when hdr % maxalign = 0 THEN maxalign ELSE hdr%maxalign END)))::numeric AS datahdr,
                           (maxfracsum * (nullhdr + maxalign - (case when nullhdr%maxalign = 0 THEN maxalign ELSE nullhdr%maxalign END))) AS nullhdr2
                       FROM (
                           SELECT
                               med.schemaname,
                               med.tablename,
                               hdr,
                               maxalign,
                               bs,
                               datawidth,
                               maxfracsum,
                               hdr + 1 + coalesce(cntt1.cnt,0) as nullhdr
                           FROM (
                               SELECT
                                   schemaname,
                                   tablename,
                                   hdr,
                                   maxalign,
                                   bs,
                                   SUM((1-s.null_frac)*s.avg_width) AS datawidth,
                                   MAX(s.null_frac) AS maxfracsum
                               FROM
                                   pg_stats_bloat_chk s,
                                   (SELECT current_setting('block_size')::numeric AS bs, 27 AS hdr, 4 AS maxalign) AS constants
                               GROUP BY 1, 2, 3, 4, 5
                           ) AS med
                           LEFT JOIN (
                               select (count(*)/8) AS cnt,schemaname,tablename from pg_stats_bloat_chk where null_frac <> 0 group by schemaname,tablename
                           ) AS cntt1
                           ON med.schemaname = cntt1.schemaname and med.tablename = cntt1.tablename
                       ) AS foo
                   ) AS rs
                   JOIN pg_class_bloat_chk cc ON cc.relname = rs.tablename

                   JOIN pg_namespace_bloat_chk nn ON cc.relnamespace = nn.oid_ss AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
               ) AS sml
               WHERE sml.relpages - live_size_blocks > 2
             ) AS blochk where wastedsize>1073741824 and bloat>2;"""

    result = run_psql(sql, stderr_devnull=True)
    if result.returncode != 0:
        error("Heap table bloat check error! \n")
        return -1

    # AO table bloat check
    info("---Start AO table bloat check...\n")
    num_proc = 0
    num_finish = 0
    itotal = len(schema_list)

    for icalc in range(itotal):
        pid = os.fork()
        if pid < 0:
            print("Can not fork a child process!!!")
            sys.exit(-1)

        if pid == 0:
            # Child process
            schema = schema_list[icalc]
            sql = (f"copy (select schemaname||'.'||tablename,'ao',bloat "
                   f"from AOtable_bloatcheck('{schema}') where bloat>1.9) "
                   f"to '/tmp/tmpaobloat.{schema}.dat';")
            res = subprocess.run(
                ["psql", "-A", "-X", "-t", "-c", sql,
                 "-h", hostname, "-p", port, "-U", username, "-d", database],
                capture_output=True, text=True, check=False,
            )
            if res.returncode != 0:
                error(f"Unload {schema} AO table error! \n{res.stderr}\n")
                os._exit(255)

            sql = f"copy bloat_skew_result from '/tmp/tmpaobloat.{schema}.dat';"
            res = run_psql(sql, stderr_devnull=True)
            if res.returncode != 0:
                error(f"Load {schema} AO bloat into bloat_skew_result error! \n")
                os._exit(255)

            os._exit(0)

        else:
            # Parent process
            num_proc += 1
            print(f"Child process count [{num_proc}], finish count[{num_finish}/{itotal}]")
            while num_proc >= concurrency:
                time.sleep(1)

    # Wait for all children
    while num_proc > 0:
        time.sleep(1)
    print(f"Child process count [{num_proc}], finish count[{num_finish}/{itotal}]")

    # Query results
    sql = "select * from bloat_skew_result order by relstorage,bloat desc;"
    result = subprocess.run(
        ["psql", "-X", "-c", sql,
         "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        error("Query bloat check result error! \n")
        return -1
    info("---Bloat check result\n")
    info_notimestr(f"\n{result.stdout}\n")
    return 0


def parallel_vacuum() -> int:
    """Run vacuum and analyze on bloated tables in parallel."""
    global num_proc, num_finish

    print(f"---Start vacuum, jobs [{concurrency}]")
    sql = "select tablename from bloat_skew_result order by bloat desc;"
    result = run_psql(sql)
    if result.returncode != 0:
        error("load bloat table result error! \n")
        return -1

    vacuumlist = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
    itotal = len(vacuumlist)
    num_proc = 0
    num_finish = 0

    for icalc in range(itotal):
        pid = os.fork()
        if pid < 0:
            print("Can not fork a child process!!!")
            sys.exit(-1)

        if pid == 0:
            # Child process
            table = vacuumlist[icalc]
            sql = f"vacuum {table}; analyze {table};"
            info(f" [{sql}]\n")
            res = run_psql(sql, stderr_devnull=True)
            if res.returncode != 0:
                error(f"vacuum {table} error! \n")
                os._exit(255)
            os._exit(0)

        else:
            # Parent process
            num_proc += 1
            print(f"Child process count [{num_proc}], finish count[{num_finish}/{itotal}]")
            while num_proc >= concurrency:
                time.sleep(1)

    # Wait for all children
    while num_proc > 0:
        time.sleep(1)
    print(f"Child process count [{num_proc}], finish count[{num_finish}/{itotal}]")
    return 0


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    global cmd_name
    cmd_name = get_cmd_name(sys.argv[0])

    if len(sys.argv) == 1:
        print(f"Input error: \nPlease show help: python3 {cmd_name} --help")
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Greenplum vacuum script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Examples:
  python3 {cmd_name} -d testdb -u gpadmin --include-schema public --include-schema gpp_sync --jobs 3
  python3 {cmd_name} -d testdb -u gpadmin --exclude-schema public --exclude-schema dw --jobs 3
""",
    )
    parser.add_argument("--hostname", "-H", default="localhost",
                        help="Master hostname or master host IP. Default: localhost")
    parser.add_argument("--port", "-p", default="5432",
                        help="GP Master port number. Default: 5432")
    parser.add_argument("--dbname", default=None,
                        help="Database name.")
    parser.add_argument("--username", default="gpadmin",
                        help="The super user of GPDB. Default: gpadmin")
    parser.add_argument("--password", "--pw", default="gpadmin",
                        help="The password of GP user. Default: no password")
    parser.add_argument("--all", "-a", action="store_true", dest="is_all",
                        help="Check all the schema in database.")
    parser.add_argument("--log-dir", "-l", default="~/gpAdminLogs",
                        help="The directory to write the log file. Default: ~/gpAdminLogs.")
    parser.add_argument("--jobs", type=int, default=2,
                        help="The number of parallel jobs to vacuum. Default: 2")
    parser.add_argument("--include-schema", "-s", action="append", default=[],
                        help="Vacuum only specified schema(s). Can be specified multiple times.")
    parser.add_argument("--include-schema-file", default="",
                        help="A file containing a list of schema to be vacuum.")
    parser.add_argument("--exclude-schema", action="append", default=[],
                        help="Exclude specified schema(s). Can be specified multiple times.")
    parser.add_argument("--exclude-schema-file", default="",
                        help="A file containing a list of schemas to be excluded.")

    args = parser.parse_args()

    # Validate mutually exclusive options
    option_count = sum([
        args.is_all,
        len(args.include_schema) > 0,
        len(args.include_schema_file) > 0,
        len(args.exclude_schema) > 0,
        len(args.exclude_schema_file) > 0,
    ])
    if option_count > 1:
        print("Input error: The following options may not be specified together: "
              "all, include-schema, include-schema-file, exclude-schema, exclude-schema-file")
        sys.exit(0)
    if option_count == 0:
        print("Input error: The following options should be specified one: "
              "all, include-schema, include-schema-file, exclude-schema, exclude-schema-file")
        sys.exit(0)

    return args


def main() -> None:
    """Main entry point."""
    global hostname, port, database, username, password, concurrency

    args = parse_args()

    hostname = args.hostname
    port = args.port
    database = args.dbname if args.dbname else os.environ.get("PGDATABASE", "postgres")
    username = args.username
    password = args.password
    concurrency = args.jobs

    set_env()
    init_log()
    info("-----------------------------------------------------\n")
    info("------Program start...\n")
    info("-----------------------------------------------------\n")

    ret = check_process()
    if ret > 1:
        info(f"There is another {cmd_name} process is running. \n")
        print(f"There is another {cmd_name} process is running.")
    if ret == 1:
        get_schema(
            args.is_all,
            args.include_schema,
            args.include_schema_file,
            args.exclude_schema,
            args.exclude_schema_file,
        )
        bloatcheck()
        parallel_vacuum()

    info("-----------------------------------------------------\n")
    info("------Finished !\n")
    info("-----------------------------------------------------\n")
    close_log()


if __name__ == "__main__":
    main()
