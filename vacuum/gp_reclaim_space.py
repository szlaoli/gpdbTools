#!/usr/bin/env python
"""Greenplum reclaim space: bloat check and table reorganization with parallel execution."""
from __future__ import print_function

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

# Globals
hostname = "localhost"
port = "5432"
database = "postgres"
username = "gpadmin"
password = "gpadmin"

schema_list = []
schema_str = ""
starttime = 0.0
gpver = 0
concurrency = 2
duration = 0.0

num_proc = 0
num_finish = 0
mainpid = os.getpid()

cmd_name = os.path.basename(sys.argv[0])


class _CmdResult(object):
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def handler(signum, frame):
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


def get_current_date():
    """Return current date as YYYYMMDD string."""
    return datetime.now().strftime("%Y%m%d")


def show_time():
    """Return current timestamp as YYYY-MM-DD HH:MM:SS string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def check_weekday(s_runday):
    """Check if today is one of the specified weekdays (1=Mon, 7=Sun). Returns 1 if match or empty."""
    if not s_runday:
        return 1
    days = s_runday.split(",")
    weekday = datetime.now().isoweekday()  # 1=Monday, 7=Sunday
    for d in days:
        if str(weekday) == d.strip():
            return 1
    return 0


def check_current_day(exdaystr):
    """Check if today's day-of-month is in the exclude list. Returns 1 if match."""
    if not exdaystr:
        return 0
    mday = datetime.now().day
    days = exdaystr.split(",")
    for d in days:
        d = d.strip()
        if d and int(d) == mday:
            return 1
    return 0


def info(printmsg):
    """Print an INFO message to stdout."""
    print("[{0} INFO] {1}".format(show_time(), printmsg), end="")
    return 0


def info_notimestr(printmsg):
    """Print a message to stdout without timestamp."""
    print(printmsg, end="")
    return 0


def error(printmsg):
    """Print an ERROR message to stdout."""
    print("[{0} ERROR] {1}".format(show_time(), printmsg), end="")
    return 0


def set_env():
    """Set PostgreSQL environment variables."""
    os.environ["PGHOST"] = hostname
    os.environ["PGPORT"] = port
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password
    return 0


def run_psql(sql, extra_args=None, stderr_devnull=False):
    """Run a psql command and return the result."""
    cmd = ["psql", "-A", "-X", "-t", "-c", sql,
           "-h", hostname, "-p", port, "-U", username, "-d", database]
    if extra_args:
        cmd.extend(extra_args)
    if stderr_devnull:
        devnull = open(os.devnull, 'w')
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=devnull)
    else:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if stderr_devnull:
        devnull.close()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if err and hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return _CmdResult(proc.returncode, out, err or '')


def get_gpver():
    """Get the major Greenplum version number."""
    sql = "select version();"
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-d", "postgres"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode != 0:
        print("Get GP version error!")
        sys.exit(1)
    sver = out.strip()
    parts = sver.split()
    if len(parts) > 4:
        print(parts[4])
        info_notimestr("GP Version: {0}\n".format(parts[4]))
        ver_parts = parts[4].split(".")
        print(ver_parts[0])
        return int(ver_parts[0])
    print("Could not parse GP version")
    sys.exit(1)


def get_schema(
    is_all,
    chk_schema,
    schema_file,
    exclude_schema,
    exclude_schema_file,
):
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
        schema_list = [s.strip() for s in chk_schema.split(",")]

    # --include-schema-file
    if schema_file:
        if not os.path.exists(schema_file):
            error("Schema file {0} do not exist!\n".format(schema_file))
            sys.exit(1)
        try:
            with open(schema_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        schema_list.append(line)
        except OSError as e:
            print("{0} open {1} error: {2}".format(show_time(), schema_file, e))
            sys.exit(1)

    # --exclude-schema
    if exclude_schema:
        exclude_list = [s.strip() for s in exclude_schema.split(",")]
        ex_str = "(" + ",".join("'{0}'".format(s) for s in exclude_list) + ")"
        print("Exclude SCHEMA: {0}".format(ex_str))
        sql = ("select nspname from pg_namespace where nspname not like 'pg%' "
               "and nspname not like 'gp%' and nspname not in {0} order by 1;".format(ex_str))
        result = run_psql(sql)
        if result.returncode != 0:
            error("Query schema name exclude file error\n")
            sys.exit(1)
        schema_list = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]

    # --exclude-schema-file
    if exclude_schema_file:
        if not os.path.exists(exclude_schema_file):
            error("Schema file {0} do not exist!\n".format(exclude_schema_file))
            sys.exit(1)
        exclude_list = []
        try:
            with open(exclude_schema_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        exclude_list.append(line)
        except OSError as e:
            print("{0} open {1} error: {2}".format(show_time(), exclude_schema_file, e))
            sys.exit(1)

        ex_str = "(" + ",".join("'{0}'".format(s) for s in exclude_list) + ")"
        print("Exclude SCHEMA: {0}".format(ex_str))
        sql = ("select nspname from pg_namespace where nspname not like 'pg%' "
               "and nspname not like 'gp%' and nspname not in {0} order by 1;".format(ex_str))
        result = run_psql(sql)
        if result.returncode != 0:
            error("Query schema name exclude file error\n")
            sys.exit(1)
        schema_list = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]

    # Build schema_str
    schema_str = "(" + ",".join("'{0}'".format(s) for s in schema_list) + ")"
    print("SCHEMA: {0}".format(schema_str))


def bloatcheck():
    """Perform bloat check on heap and AO tables."""
    global num_proc, num_finish

    print("---Start bloat check, jobs [{0}]".format(concurrency))

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

    # GP version specific SQL
    if gpver >= 7:
        pg_class_sql = "insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relam=2;"
    else:
        pg_class_sql = "insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';"

    # Heap table bloat check
    info("---Start heap table bloat check...\n")
    sql = """drop table if exists pg_stats_bloat_chk;
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

             {pg_class_sql}

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
             ) AS blochk where wastedsize>1073741824 and bloat>1.9;""".format(
        pg_class_sql=pg_class_sql, schema_str=schema_str)

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
            sql = ("copy (select schemaname||'.'||tablename,'ao',bloat "
                   "from AOtable_bloatcheck('{0}') where bloat>1.9) "
                   "to '/tmp/tmpaobloat.{0}.dat';".format(schema))
            result = run_psql(sql, stderr_devnull=True)
            if result.returncode != 0:
                error("Unload {0} AO table error! \n".format(schema))
                os._exit(255)

            sql = "copy bloat_skew_result from '/tmp/tmpaobloat.{0}.dat';".format(schema)
            result = run_psql(sql, stderr_devnull=True)
            if result.returncode != 0:
                error("Load {0} AO bloat into bloat_skew_result error! \n".format(schema))
                os._exit(255)

            os._exit(0)

        else:
            # Parent process
            num_proc += 1
            print("Child process count [{0}], finish count[{1}/{2}]".format(num_proc, num_finish, itotal))
            while num_proc >= concurrency:
                time.sleep(1)

    # Wait for all children
    while num_proc > 0:
        time.sleep(1)
    print("Child process count [{0}], finish count[{1}/{2}]".format(num_proc, num_finish, itotal))

    # Query results
    sql = "select * from bloat_skew_result order by relstorage,bloat desc;"
    proc = subprocess.Popen(
        ["psql", "-X", "-c", sql,
         "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    result = _CmdResult(proc.returncode, out, err or '')
    if result.returncode != 0:
        error("Query bloat check result error! \n")
        return -1
    info("---Bloat check result\n")
    info_notimestr("\n{0}\n".format(result.stdout))
    return 0


def parallel_run():
    """Reclaim space by reorganizing bloated tables in parallel."""
    global num_proc, num_finish

    print("---Start reclaim space, jobs [{0}]".format(concurrency))
    sql = "select tablename from bloat_skew_result order by bloat desc;"
    result = run_psql(sql)
    if result.returncode != 0:
        error("load bloat table result error! \n")
        return -1

    reclaimlist = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
    itotal = len(reclaimlist)
    num_proc = 0
    num_finish = 0

    for icalc in range(itotal):
        nowtime = time.time()
        t_interval = nowtime - starttime
        if duration > 0 and t_interval > duration * 3600:
            info("Program is time out, stopping now!\n")
            break

        pid = os.fork()
        if pid < 0:
            print("Can not fork a child process!!!")
            sys.exit(-1)

        if pid == 0:
            # Child process
            table = reclaimlist[icalc]
            sql = "alter table {0} set with (reorganize=true);".format(table)
            info(" [{0}]\n".format(sql))
            result = run_psql(sql, stderr_devnull=True)
            if result.returncode != 0:
                error("alter table {0} error! \n[{1}]\n".format(table, result.stdout))
                os._exit(255)
            os._exit(0)

        else:
            # Parent process
            num_proc += 1
            print("Child process count [{0}], finish count[{1}/{2}]".format(num_proc, num_finish, itotal))
            while num_proc >= concurrency:
                time.sleep(1)

    # Wait for all children
    while num_proc > 0:
        time.sleep(1)
    print("Child process count [{0}], finish count[{1}/{2}]".format(num_proc, num_finish, itotal))
    return 0


def parse_args():
    """Parse command line arguments."""
    if len(sys.argv) == 1:
        print("Input error: \nPlease show help: python3 {0} --help".format(cmd_name))
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Greenplum reclaim space script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 {0} -d testdb -u gpadmin --include-schema gpp_sync,syndata --jobs 3
  python3 {0} -d testdb -u gpadmin --include-schema-file /tmp/schema.conf --jobs 3
  python3 {0} -d testdb -u gpadmin --exclude-schema dw,public --jobs 3
  python3 {0} -d testdb -u gpadmin -s gpp_sync,syndata --jobs 3 --week-day 6,7 --exclude-date 1,2,5,6 --duration 2
""".format(cmd_name),
    )
    parser.add_argument("--hostname", "-H", default="localhost",
                        help="Master hostname or master host IP. Default: localhost")
    parser.add_argument("--port", "-p", default="5432",
                        help="GP Master port number. Default: 5432")
    parser.add_argument("--dbname", "-d", default=None,
                        help="Database name.")
    parser.add_argument("--username", "-u", default="gpadmin",
                        help="The super user of GPDB. Default: gpadmin")
    parser.add_argument("--password", "--pw", default="gpadmin",
                        help="The password of GP user. Default: no password")
    parser.add_argument("--all", "-a", action="store_true", dest="is_all",
                        help="Check all the schema in database.")
    parser.add_argument("--jobs", type=int, default=2,
                        help="The number of parallel jobs to vacuum. Default: 2")
    parser.add_argument("--include-schema", "-s", default="",
                        help="Vacuum only specified schema(s). Example: dw,dm,ods")
    parser.add_argument("--include-schema-file", default="",
                        help="A file containing a list of schema to be vacuum.")
    parser.add_argument("--exclude-schema", "-e", default="",
                        help="Exclude specified schema(s). Example: dw,dm,ods")
    parser.add_argument("--exclude-schema-file", default="",
                        help="A file containing a list of schemas to be excluded.")
    parser.add_argument("--week-day", default="",
                        help="Run on specified days of week. Example: 6,7")
    parser.add_argument("--exclude-date", default="",
                        help="Do not run on specified days of month. Example: 1,2,5,6")
    parser.add_argument("--duration", type=float, default=0,
                        help="Duration in hours. Example: 1 for one hour, 0.5 for half an hour.")

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
    if args.jobs <= 0:
        print("Input error: --jobs <parallel_job_number>\n"
              "  The number of parallel jobs to healthcheck, include: skew, bloat. Default: 2")
        sys.exit(0)

    return args


def main():
    """Main entry point."""
    global hostname, port, database, username, password
    global concurrency, duration, starttime, gpver

    args = parse_args()

    hostname = args.hostname
    port = args.port
    database = args.dbname if args.dbname else os.environ.get("PGDATABASE", "postgres")
    username = args.username
    password = args.password
    concurrency = args.jobs
    duration = args.duration

    set_env()
    gpver = get_gpver()

    chkret1 = check_weekday(args.week_day)
    chkret2 = check_current_day(args.exclude_date)

    if chkret2:
        info("Today is {0}. Program stopped!\n".format(get_current_date()))
        sys.exit(0)
    if chkret1 == 0:
        info("Today is not {0} of week. Program stopped!\n".format(args.week_day))
        sys.exit(0)

    info("-----------------------------------------------------\n")
    info("------Program start...\n")
    info("-----------------------------------------------------\n")

    starttime = time.time()
    get_schema(
        args.is_all,
        args.include_schema,
        args.include_schema_file,
        args.exclude_schema,
        args.exclude_schema_file,
    )
    bloatcheck()
    parallel_run()

    info("-----------------------------------------------------\n")
    info("------Finished !\n")
    info("-----------------------------------------------------\n")


if __name__ == "__main__":
    main()
