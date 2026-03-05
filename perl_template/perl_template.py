#!/usr/bin/env python3
"""Greenplum template script with option parsing, logging, schema queries, and gpstate check."""

import argparse
import os
import subprocess
import sys
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
fh_log: Optional[TextIOWrapper] = None
cmd_name: str = os.path.basename(sys.argv[0])


def get_current_date() -> str:
    """Return current date as YYYYMMDD string."""
    now = datetime.now()
    return now.strftime("%Y%m%d")


def show_time() -> str:
    """Return current timestamp as YYYY-MM-DD HH:MM:SS string."""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


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


def get_schema(
    is_all: bool,
    chk_schema: list[str],
    schema_file: str,
) -> None:
    """Populate schema_list based on CLI options."""
    global schema_list

    # --all
    if is_all:
        sql = "select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' order by 1;"
        result = subprocess.run(
            ["psql", "-A", "-X", "-t", "-c", sql,
             "-h", hostname, "-p", port, "-U", username, "-d", database],
            capture_output=True, text=True, check=False,
        )
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


def gpstate() -> None:
    """Run gpstate commands and query gp_configuration_history."""
    info("---gpstate\n")

    result = subprocess.run(["gpstate", "-e"], capture_output=True, text=True, check=False)
    info_notimestr(f"\n{result.stdout}\n")

    result = subprocess.run(["gpstate", "-f"], capture_output=True, text=True, check=False)
    info_notimestr(f"{result.stdout}\n")

    sql = "select * from gp_configuration_history order by 1 desc limit 50;"
    result = subprocess.run(
        ["psql", "-A", "-X", "-t", "-c", sql,
         "-h", hostname, "-p", port, "-U", username, "-d", database],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        error("Get gp_configuration_history error\n")
        return
    info("---gp_configuration_history\n")
    info_notimestr(f"{result.stdout}\n")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    if len(sys.argv) == 1:
        print(f"Input error: \nPlease show help: python3 {cmd_name} --help")
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Greenplum template script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Examples:
  python3 {cmd_name} --dbname testdb --all --jobs 3
  python3 {cmd_name} --dbname testdb --include-schema public --include-schema gpp_sync
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


def main() -> None:
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
