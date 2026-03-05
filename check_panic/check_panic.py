#!/usr/bin/env python
from __future__ import print_function

import argparse
import os
import subprocess
import sys
import tempfile
from datetime import datetime


cmd_name = os.path.basename(sys.argv[0])
fh_log = None
logfilename = ""
gpver = ""


def get_current_date():
    return datetime.now().strftime("%Y%m%d")


def show_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_log():
    global fh_log, logfilename
    logday = get_current_date()
    home = os.environ.get("HOME", "")
    logfilename = "{}/gpAdminLogs/{}_{}.log".format(home, cmd_name, logday)
    try:
        fh_log = open(logfilename, "a")
    except OSError:
        print("[ERROR]:Cound not open logfile {}".format(logfilename))
        sys.exit(-1)


def info(printmsg):
    if fh_log:
        fh_log.write("[{} INFO] {}".format(show_time(), printmsg))
    return 0


def info_notimestr(printmsg):
    if fh_log:
        fh_log.write(printmsg)
    return 0


def error(printmsg):
    if fh_log:
        fh_log.write("[{} ERROR] {}".format(show_time(), printmsg))
    return 0


def close_log():
    if fh_log:
        fh_log.close()


def set_env(hostname, port, database, username, password):
    os.environ["PGHOST"] = hostname
    os.environ["PGPORT"] = port
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = password


def get_gpver():
    sql = "select version();"
    proc = subprocess.Popen(
        ["psql", "-A", "-X", "-t", "-c", sql, "-d", "postgres"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        print("Get GP version error!")
        sys.exit(1)
    sver = out.strip()
    tmpstr = sver.split(" ")
    print(tmpstr[4])
    tmpver = tmpstr[4].split(".")
    print(tmpver[0])
    return tmpver[0]


def run_psql(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return proc.returncode, out


def check_panic_on_allhost(
    hostname, port, username, database, chk_date
):
    sql = """
  DROP EXTERNAL TABLE IF EXISTS check_panic_on_seg_ext;
  CREATE EXTERNAL WEB TABLE check_panic_on_seg_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''\\"PANIC\\"'' $\\GP_SEG_DATADIR/pg_log/gpdb-{chk_date}*csv|grep -v ''EXECUTE E''|sed  ''1,$ s/^/''$\\HOSTN'':/g''' ON ALL
  FORMAT 'TEXT' (DELIMITER E'\\x2');

  DROP EXTERNAL TABLE IF EXISTS check_panic_on_master_ext;
  CREATE EXTERNAL WEB TABLE check_panic_on_master_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''\\"PANIC\\"'' $\\GP_SEG_DATADIR/pg_log/gpdb-{chk_date}*csv|grep -v ''EXECUTE E''|sed  ''1,$ s/^/''$\\HOSTN'':/g''' ON MASTER
  FORMAT 'TEXT' (DELIMITER E'\\x2');

  DROP TABLE IF EXISTS check_panic;
  CREATE TABLE check_panic (
    hostname text,
    logfilename text,
    logtime text,
    pid int,
    sess_id text,
    logmsg text
  ) distributed randomly;

  INSERT INTO check_panic
  select
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  substr(split_part(log_msg,',',4),2,length(split_part(log_msg,',',4))-1)::int pid,
  split_part(log_msg,',',10) sess_id,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_panic_on_seg_ext;

  INSERT INTO check_panic
  select
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  substr(split_part(log_msg,',',4),2,length(split_part(log_msg,',',4))-1)::int pid,
  split_part(log_msg,',',10) sess_id,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_panic_on_master_ext;


  DROP EXTERNAL TABLE IF EXISTS check_terminate_on_seg_ext;
  CREATE EXTERNAL WEB TABLE check_terminate_on_seg_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''was terminated by signal'' $\\GP_SEG_DATADIR/pg_log/gpdb-{chk_date}*csv|grep -v ''EXECUTE E''|sed  ''1,$ s/^/''$\\HOSTN'':/g''' ON ALL
  FORMAT 'TEXT' (DELIMITER E'\\x2');

  DROP EXTERNAL TABLE IF EXISTS check_terminate_on_master_ext;
  CREATE EXTERNAL WEB TABLE check_terminate_on_master_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''was terminated by signal'' $\\GP_SEG_DATADIR/pg_log/gpdb-{chk_date}*csv|grep -v ''EXECUTE E''|sed  ''1,$ s/^/''$\\HOSTN'':/g''' ON MASTER
  FORMAT 'TEXT' (DELIMITER E'\\x2');

  DROP TABLE IF EXISTS check_terminate;
  CREATE TABLE check_terminate (
    hostname text,
    logfilename text,
    logtime text,
    pid int,
    logmsg text
  ) distributed randomly;

  INSERT INTO check_terminate
  select
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  split_part(split_part(split_part(log_msg,'(',2),')',1),' ',2)::int pid,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_terminate_on_seg_ext;

  INSERT INTO check_terminate
  select
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  split_part(split_part(split_part(log_msg,'(',2),')',1),' ',2)::int pid,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_terminate_on_master_ext;
  """.format(chk_date=chk_date)

    tmpfile = "/tmp/.tmpsqlfile.sql"
    try:
        with open(tmpfile, "w") as f:
            f.write(sql)
    except OSError:
        print("[ERROR]:Cound not open sqlfile /tmp/.tmpsqlfile.sql")
        return

    ret, _ = run_psql([
        "psql", "-A", "-X", "-t", "-f", tmpfile,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Check PANIC on all hosts error! \n")
        return

    # Check PANIC messages
    info("\n---Check PANIC messages---\n")
    print("\n---Check PANIC messages---")

    sql = "select count(*) from check_panic;"
    ret, check_panic_count_str = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Check PANIC rows error! \n")
        return
    check_panic_count = int(check_panic_count_str.strip())

    sess_list = []
    if check_panic_count > 0:
        sql = "select * from check_panic order by 1,2,3;"
        ret, panic_info = run_psql([
            "psql", "-X", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Query PANIC information error! \n")
            return
        info_notimestr("\n{}\n".format(panic_info))
        print("\n{}".format(panic_info))

        sql = "select distinct sess_id from check_panic;"
        ret, sess_output = run_psql([
            "psql", "-A", "-X", "-t", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Load session list error! \n")
            return
        sess_list = [s for s in sess_output.strip().split("\n") if s]
    else:
        info_notimestr("No PANIC in pg_log\n")
        print("No PANIC in pg_log")

    # Check process terminated without PANIC
    info("\n---Check process terminated without PANIC---\n")
    print("\n---Check process terminated without PANIC---")

    sql = ("select count(*) from check_terminate a left join check_panic b "
           "on a.hostname=b.hostname and a.logfilename=b.logfilename and a.pid=b.pid "
           "where b.hostname is null;")
    ret, check_terminate_count_str = run_psql([
        "psql", "-A", "-X", "-t", "-c", sql,
        "-h", hostname, "-p", port, "-U", username, "-d", database
    ])
    if ret:
        error("Check terminate rows error! \n")
        return
    check_terminate_count = int(check_terminate_count_str.strip())

    pid_list = []
    if check_terminate_count > 0:
        sql = ("select * from check_terminate a left join check_panic b "
               "on a.hostname=b.hostname and a.logfilename=b.logfilename and a.pid=b.pid "
               "where b.hostname is null order by 1,2,3;")
        ret, terminate_info = run_psql([
            "psql", "-X", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Query process terminate information error! \n")
            return
        info_notimestr("\n{}\n".format(terminate_info))
        print("\n{}".format(terminate_info))

        sql = ("select a.hostname||'|'||a.logfilename||'|'||to_char(a.logtime::timestamp,'YYYY-MM-DD HH24:MI:SS')||'|p'||a.pid::text "
               "from check_terminate a left join check_panic b on a.hostname=b.hostname and a.logfilename=b.logfilename and a.pid=b.pid "
               "where b.hostname is null "
               "group by a.hostname,a.logfilename,a.logtime,a.pid order by 1;")
        ret, pid_output = run_psql([
            "psql", "-A", "-X", "-t", "-c", sql,
            "-h", hostname, "-p", port, "-U", username, "-d", database
        ])
        if ret:
            error("Load pid list error! \n")
            return
        pid_list = [s for s in pid_output.strip().split("\n") if s]
    else:
        info_notimestr("No process terminated info in pg_log\n")
        print("No process terminated info in pg_log")

    if check_panic_count == 0 and check_terminate_count == 0:
        sys.exit(0)

    master_data_dir = os.environ.get("MASTER_DATA_DIRECTORY", "")

    # Show PANIC LOG
    if sess_list:
        info_notimestr("\n-------------Show PANIC LOG--------------\n")
        print("\n-------------Show PANIC LOG--------------")
        for sess_id in sess_list:
            sess_id = sess_id.strip()
            info_notimestr("---===Session ID: {}===---\n".format(sess_id))
            print("---===Session ID: {}===---".format(sess_id))
            devnull = open(os.devnull, 'w')
            proc = subprocess.Popen(
                "gplogfilter -f '{sess_id}' {master_data_dir}/pg_log/gpdb-{chk_date}*csv 2>/dev/null |tail -100".format(
                    sess_id=sess_id, master_data_dir=master_data_dir, chk_date=chk_date
                ),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            devnull.close()
            if hasattr(out, 'decode'):
                out = out.decode('utf-8', 'replace')
            showlogmsg = out
            info_notimestr("{}\n".format(showlogmsg))
            print(showlogmsg)

    if pid_list:
        info_notimestr("\n-------------Show process terminated LOG--------------\n")
        print("\n-------------Show process terminated LOG--------------")
        for pid_entry in pid_list:
            tmpstr = pid_entry.strip().split("|")
            info_notimestr("---===Hostname: {}, Logfilename: {}, PID: {}===---\n".format(tmpstr[0], tmpstr[1], tmpstr[3]))
            print("---===Hostname: {}, Logfilename: {}, PID: {}===---".format(tmpstr[0], tmpstr[1], tmpstr[3]))
            proc = subprocess.Popen(
                "ssh {hostname} \"gplogfilter -f '{pid}' -e '{logtime}' {logfile} 2>/dev/null |tail -50\"".format(
                    hostname=tmpstr[0], pid=tmpstr[3], logtime=tmpstr[2], logfile=tmpstr[1]
                ),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            if hasattr(out, 'decode'):
                out = out.decode('utf-8', 'replace')
            showlogmsg = out
            info_notimestr("{}\n".format(showlogmsg))
            print(showlogmsg)


def main():
    global gpver

    parser = argparse.ArgumentParser(
        description="Check PANIC and terminated messages in GP log files",
        add_help=False
    )
    parser.add_argument("--hostname", "-h", default="localhost",
                        help="Master hostname or master host IP. Default: localhost")
    parser.add_argument("--port", "-p", default="5432",
                        help="GP Master port number. Default: 5432")
    parser.add_argument("--dbname", "-d", default=None,
                        help="Database name.")
    parser.add_argument("--username", "-u", default="gpadmin",
                        help="The super user of GPDB. Default: gpadmin")
    parser.add_argument("--password", "-pw", default="gpadmin",
                        help="The password of GP user. Default: no password")
    parser.add_argument("--help", "-?", action="store_true", dest="show_help",
                        help="Show the help message.")
    parser.add_argument("--check_date", default=None,
                        help="Checking PANIC date, format: YYYY-MM-DD / YYYY-MM")

    if len(sys.argv) == 1:
        print("Input error: \nPlease show help: python {} --help".format(cmd_name))
        sys.exit(0)

    args = parser.parse_args()

    if args.show_help:
        parser.print_help()
        sys.exit(0)

    hostname = args.hostname
    port = args.port
    database = args.dbname
    username = args.username
    password = args.password
    chk_date = args.check_date

    if database is None:
        pgdatabase = os.environ.get("PGDATABASE", "")
        if pgdatabase:
            database = pgdatabase
        else:
            database = "postgres"

    if chk_date is None or (len(chk_date) != 10 and len(chk_date) != 7):
        print("Input error: check_date format: YYYY-MM-DD / YYYY-MM\nPlease show help: python {} --help".format(cmd_name))
        sys.exit(0)

    set_env(hostname, port, database, username, password)
    init_log()
    gpver = get_gpver()

    info("---------------------------------------------------------------------------------------\n")
    info("------Check PANIC and terminated info from pg_log\n")
    info("---------------------------------------------------------------------------------------\n")
    print("---------------------------------------------------------------------------------------")
    print("------Check PANIC and terminated info from pg_log")
    print("---------------------------------------------------------------------------------------")

    check_panic_on_allhost(hostname, port, username, database, chk_date)

    print("---------------------------------------------------------------------------------------")
    print("------Check {} for more detail info.".format(logfilename))
    print("---------------------------------------------------------------------------------------")
    close_log()


if __name__ == "__main__":
    main()
