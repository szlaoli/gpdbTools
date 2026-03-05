#!/usr/bin/env python
from __future__ import print_function
import os
import subprocess
import sys
from datetime import datetime


RM_INTERVAL = 30
GZIP_INTERVAL = 5
fh_log = None
master_data_directory = ""


def get_current_date():
    return datetime.now().strftime("%Y%m%d")


def show_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_log():
    global fh_log
    logday = get_current_date()
    home = os.environ.get("HOME", "")
    logpath = "{}/gpAdminLogs/clean_log_{}.log".format(home, logday)
    try:
        fh_log = open(logpath, "a")
    except OSError:
        print("[ERROR]:Cound not open logfile {}".format(logpath))
        sys.exit(-1)


def info(printmsg):
    if fh_log:
        fh_log.write("[{} INFO] {}".format(show_time(), printmsg))
    return 0


def error(printmsg):
    if fh_log:
        fh_log.write("[{} ERROR] {}".format(show_time(), printmsg))
    return 0


def run_cmd(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return out


def gpenv():
    global master_data_directory

    proc = subprocess.Popen(
        ["bash", "-c", "source ~/.bashrc; env"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    config_params = {}
    for line in out.splitlines():
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


def main():
    gpenv()
    init_log()

    info("------gpAdminLogs rm list------\n")
    rmlist = run_cmd(
        'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{} -name \'*.log*\' -exec ls -l {{}} \\;"'.format(RM_INTERVAL)
    )
    info(rmlist)
    run_cmd(
        'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{} -name \'*.log*\' -exec rm -f {{}} \\;"'.format(RM_INTERVAL)
    )

    info("------gpAdminLogs gzip list------\n")
    gziplist = run_cmd(
        'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{} -name \'*.log\' -exec ls -l {{}} \\;"'.format(GZIP_INTERVAL)
    )
    info(gziplist)
    run_cmd(
        'gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +{} -name \'*.log\' -exec gzip -f {{}} \\;"'.format(GZIP_INTERVAL)
    )

    info("------Master pg_log rm list------\n")
    rmlist = run_cmd(
        'gpssh -f ~/allmasters "find {}/pg_log -mtime +{} -name \'*.csv*\' -exec ls -l {{}} \\;"'.format(master_data_directory, RM_INTERVAL)
    )
    info(rmlist)
    run_cmd(
        'gpssh -f ~/allmasters "find {}/pg_log -mtime +{} -name \'*.csv*\' -exec rm -f {{}} \\;"'.format(master_data_directory, RM_INTERVAL)
    )

    info("------Master pg_log gzip list------\n")
    gziplist = run_cmd(
        'gpssh -f ~/allmasters "find {}/pg_log -mtime +{} -name \'*.csv\' -exec ls -l {{}} \\;"'.format(master_data_directory, GZIP_INTERVAL)
    )
    info(gziplist)
    run_cmd(
        'gpssh -f ~/allmasters "find {}/pg_log -mtime +{} -name \'*.csv\' -exec gzip -f {{}} \\;"'.format(master_data_directory, GZIP_INTERVAL)
    )

    info("------Segment pg_log rm list------\n")
    rmlist = run_cmd(
        'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{} -name \'*.csv*\' -exec ls -l {{}} \\;"'.format(RM_INTERVAL)
    )
    info(rmlist)
    run_cmd(
        'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{} -name \'*.csv*\' -exec rm -f {{}} \\;"'.format(RM_INTERVAL)
    )
    rmlist = run_cmd(
        'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{} -name \'*.csv*\' -exec ls -l {{}} \\;"'.format(RM_INTERVAL)
    )
    info(rmlist)
    run_cmd(
        'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{} -name \'*.csv*\' -exec rm -f {{}} \\;"'.format(RM_INTERVAL)
    )

    info("------Segment pg_log gzip list------\n")
    gziplist = run_cmd(
        'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{} -name \'*.csv\' -exec ls -l {{}} \\;"'.format(GZIP_INTERVAL)
    )
    info(gziplist)
    run_cmd(
        'gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +{} -name \'*.csv\' -exec gzip -f {{}} \\;"'.format(GZIP_INTERVAL)
    )
    gziplist = run_cmd(
        'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{} -name \'*.csv\' -exec ls -l {{}} \\;"'.format(GZIP_INTERVAL)
    )
    info(gziplist)
    run_cmd(
        'gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +{} -name \'*.csv\' -exec gzip -f {{}} \\;"'.format(GZIP_INTERVAL)
    )

    if fh_log:
        fh_log.close()


if __name__ == "__main__":
    main()
