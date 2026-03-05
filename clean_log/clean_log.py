#!/usr/bin/env python3
import os
import subprocess
import sys
from datetime import datetime
from typing import IO, Optional


RM_INTERVAL: int = 30
GZIP_INTERVAL: int = 5
fh_log: Optional[IO[str]] = None
master_data_directory: str = ""


def get_current_date() -> str:
    return datetime.now().strftime("%Y%m%d")


def show_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_log() -> None:
    global fh_log
    logday = get_current_date()
    home = os.environ.get("HOME", "")
    logpath = f"{home}/gpAdminLogs/clean_log_{logday}.log"
    try:
        fh_log = open(logpath, "a")
    except OSError:
        print(f"[ERROR]:Cound not open logfile {logpath}")
        sys.exit(-1)


def info(printmsg: str) -> int:
    if fh_log:
        fh_log.write(f"[{show_time()} INFO] {printmsg}")
    return 0


def error(printmsg: str) -> int:
    if fh_log:
        fh_log.write(f"[{show_time()} ERROR] {printmsg}")
    return 0


def run_cmd(cmd: str) -> str:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout


def gpenv() -> None:
    global master_data_directory

    result = subprocess.run(
        ["bash", "-c", "source ~/.bashrc; env"],
        capture_output=True, text=True
    )
    config_params: dict[str, str] = {}
    for line in result.stdout.splitlines():
        parts = line.split("=", 1)
        if len(parts) == 2:
            config_params[parts[0]] = parts[1]

    env_vars = [
        "GPHOME", "PATH", "LD_LIBRARY_PATH", "PYTHONPATH",
        "PYTHONHOME", "OPENSSL_CONF", "MASTER_DATA_DIRECTORY"
    ]
    for var in env_vars:
        val = config_params.get(var, "")
        if val:
            os.environ[var] = val

    master_data_directory = config_params.get("MASTER_DATA_DIRECTORY", "")


def main() -> None:
    gpenv()
    init_log()

    info("------gpAdminLogs rm list------\n")
    rmlist = run_cmd(
        f'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{RM_INTERVAL} -name \'*.log*\' -exec ls -l {{}} \\;"'
    )
    info(rmlist)
    run_cmd(
        f'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{RM_INTERVAL} -name \'*.log*\' -exec rm -f {{}} \\;"'
    )

    info("------gpAdminLogs gzip list------\n")
    gziplist = run_cmd(
        f'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{GZIP_INTERVAL} -name \'*.log\' -exec ls -l {{}} \\;"'
    )
    info(gziplist)
    run_cmd(
        f'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{GZIP_INTERVAL} -name \'*.log\' -exec gzip -f {{}} \\;"'
    )

    info("------Master pg_log rm list------\n")
    rmlist = run_cmd(
        f'gpssh -f ~/allmasters "find {master_data_directory}/pg_log -mtime +{RM_INTERVAL} -name \'*.csv*\' -exec ls -l {{}} \\;"'
    )
    info(rmlist)
    run_cmd(
        f'gpssh -f ~/allmasters "find {master_data_directory}/pg_log -mtime +{RM_INTERVAL} -name \'*.csv*\' -exec rm -f {{}} \\;"'
    )

    info("------Master pg_log gzip list------\n")
    gziplist = run_cmd(
        f'gpssh -f ~/allmasters "find {master_data_directory}/pg_log -mtime +{GZIP_INTERVAL} -name \'*.csv\' -exec ls -l {{}} \\;"'
    )
    info(gziplist)
    run_cmd(
        f'gpssh -f ~/allmasters "find {master_data_directory}/pg_log -mtime +{GZIP_INTERVAL} -name \'*.csv\' -exec gzip -f {{}} \\;"'
    )

    info("------Segment pg_log rm list------\n")
    rmlist = run_cmd(
        f'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{RM_INTERVAL} -name \'*.csv*\' -exec ls -l {{}} \\;"'
    )
    info(rmlist)
    run_cmd(
        f'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{RM_INTERVAL} -name \'*.csv*\' -exec rm -f {{}} \\;"'
    )
    rmlist = run_cmd(
        f'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{RM_INTERVAL} -name \'*.csv*\' -exec ls -l {{}} \\;"'
    )
    info(rmlist)
    run_cmd(
        f'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{RM_INTERVAL} -name \'*.csv*\' -exec rm -f {{}} \\;"'
    )

    info("------Segment pg_log gzip list------\n")
    gziplist = run_cmd(
        f'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{GZIP_INTERVAL} -name \'*.csv\' -exec ls -l {{}} \\;"'
    )
    info(gziplist)
    run_cmd(
        f'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{GZIP_INTERVAL} -name \'*.csv\' -exec gzip -f {{}} \\;"'
    )
    gziplist = run_cmd(
        f'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{GZIP_INTERVAL} -name \'*.csv\' -exec ls -l {{}} \\;"'
    )
    info(gziplist)
    run_cmd(
        f'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{GZIP_INTERVAL} -name \'*.csv\' -exec gzip -f {{}} \\;"'
    )

    if fh_log:
        fh_log.close()


if __name__ == "__main__":
    main()
