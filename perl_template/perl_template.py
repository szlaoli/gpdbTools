#!/usr/bin/env python
from __future__ import print_function
"""Greenplum template script with option parsing, logging, schema queries, and gpstate check."""

import argparse
import os
import subprocess
import sys
from datetime import datetime


class _CmdResult(object):
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


# Globals
hostname = "localhost"
port = "5432"
database = "postgres"
username = "gpadmin"
password = "gpadmin"

schema_list = []
fh_log = None
cmd_name = os.path.basename(sys.argv[0])


def get_current_date():
    """Return current date as YYYYMMDD string."""
    now = datetime.now()
    return now.strftime("%Y%m%d")


def show_time():
    """Return current timestamp as YYYY-MM-DD HH:MM:SS string."""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def init_log():
    """Open the log file for appending."""
    global fh_log
    logday = get_current_date()
    log_dir = os.path.expanduser("~/gpAdminLogs")
    log_path = os.path.join(log_dir, "{0}_{1}.log".format(cmd_name, logday))
    try:
        try:
            os.makedirs(log_dir)
        except OSError:
            if not os.path.isdir(log_dir):
                raise
        fh_log = open(log_path, "a")
    except OSError:
        print("[ERROR]:Could not open logfile {0}".format(log_path))
        sys.exit(-1)


def info(printmsg):
    """Write an INFO message to the log file."""
    if fh_log is not None:
        fh_log.write("[{0} INFO] {1}".format(show_time(), printmsg))
    return 0


def info_notimestr(printmsg):
    """Write a message to the log file without timestamp prefix."""
    if fh_log is not None:
        fh_log.write(printmsg)
    return 0


def error(printmsg):
    """Write an ERROR message to the log file."""
    if fh_log is not None:
        fh_log.write("[{0} ERROR] {1}".format(show_time(), printmsg))
    return 0


def close_log():
    """Close the log file."""
    if fh_log is not None:
        fh_log.close()
    return 0


def set_env():
    """Set PostgreSQL environment variables."""
    os.environ["PGHOST"] = hostname
    os.environ["PGPORT"] = port
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password
    return 0


def get_schema(
    is_all,
    chk_schema,
    schema_file,
):
    """Populate schema_list based on CLI options."""
    global schema_list

    # --all
    if is_all:
        sql = "select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' order by 1;"
        proc = subprocess.Popen(
            ["psql", "-A", "-X", "-t", "-c", sql,
             "-h", hostname, "-p", port, "-U", username, "-d", database],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if hasattr(out, 'decode'):
            out = out.decode('utf-8', 'replace')
        if hasattr(err, 'decode'):
            err = err.decode('utf-8', 'replace')
        result = _CmdResult(proc.returncode, out, err)
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


def gpstate():
    """Run gpstate commands and query gp_configuration_history."""
    info("---gpstate\n")

    proc = subprocess.Popen(["gpstate", "-e"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    result = _CmdResult(proc.returncode, out, err)
    info_notimestr("\n{0}\n".format(result.stdout))

    proc = subprocess.Popen(["gpstate", "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    result = _CmdResult(proc.returncode, out, err)
    info_notimestr("{0}\n".format(result.stdout))

    sql = "select * from gp_configuration_history order by 1 desc limit 50;"
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql,
         "-h", hostname, "-p", port, "-U", username, "-d", database],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    result = _CmdResult(proc.returncode, out, err)
    if result.returncode != 0:
        error("Get gp_configuration_history error\n")
        return
    info("---gp_configuration_history\n")
    info_notimestr("{0}\n".format(result.stdout))


def parse_args():
    """Parse command line arguments."""
    if len(sys.argv) == 1:
        print("Input error: \nPlease show help: python3 {0} --help".format(cmd_name))
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Greenplum template script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python3 {0} --dbname testdb --all --jobs 3\n"
               "  python3 {0} --dbname testdb --include-schema public --include-schema gpp_sync\n".format(cmd_name),
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
                        help="The number of parallel jobs. Default: 2")
    parser.add_argument("--include-schema", "-s", action="append", default=[],
                        help="Check only specified schema(s). Can be specified multiple times.")
    parser.add_argument("--include-schema-file", default="",
                        help="A file containing a list of schema to be included.")
    parser.add_argument("--global-info-only", action="store_true",
                        help="Check and output the global information of GPDB.")

    args = parser.parse_args()

    # Validate mutually exclusive options
    option_count = sum([
        args.is_all,
        len(args.include_schema) > 0,
        len(args.include_schema_file) > 0,
    ])
    if option_count > 1:
        print("Input error: The following options may not be specified together: "
              "all, include-schema, include-schema-file")
        sys.exit(0)

    return args


def main():
    """Main entry point."""
    global hostname, port, database, username, password

    args = parse_args()

    hostname = args.hostname
    port = args.port
    database = args.dbname if args.dbname else os.environ.get("PGDATABASE", "postgres")
    username = args.username
    password = args.password

    set_env()
    init_log()
    info("-----------------------------------------------------\n")
    info("------Program start\n")
    info("-----------------------------------------------------\n")

    get_schema(args.is_all, args.include_schema, args.include_schema_file)
    gpstate()

    info("-----------------------------------------------------\n")
    info("------Program Finished!\n")
    info("-----------------------------------------------------\n")
    close_log()


if __name__ == "__main__":
    main()
