#!/usr/bin/env python
from __future__ import print_function
import subprocess
import sys
from datetime import datetime


def get_current_datetime():
    return datetime.now().strftime("%Y%m%d%H%M%S")


def get_gpver():
    sql = "select version();"
    cmd = "PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c \"{}\" -d postgres".format(sql)
    proc = subprocess.Popen(
        ["bash", "-c", cmd],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
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


def get_seg_dir(iflag):
    gpver = get_gpver()

    if iflag == 0:
        if int(gpver) >= 6:
            sql = """
              SELECT conf.hostname||','||conf.datadir
              FROM gp_segment_configuration conf
              ORDER BY conf.dbid;
            """
        else:
            sql = """
              SELECT conf.hostname||','||pgfse.fselocation
              FROM pg_filespace_entry pgfse, gp_segment_configuration conf
              WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
              ORDER BY conf.dbid;
            """
    elif iflag == 1:
        if int(gpver) >= 6:
            sql = """
              SELECT conf.hostname||','||conf.datadir
              FROM gp_segment_configuration conf
              WHERE conf.content=-1 AND conf.dbid=1;
            """
        else:
            sql = """
              SELECT conf.hostname||','||pgfse.fselocation
              FROM pg_filespace_entry pgfse, gp_segment_configuration conf
              WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
              AND conf.content=-1 AND conf.dbid=1;
            """
    elif iflag == 2:
        if int(gpver) >= 6:
            sql = """
              SELECT conf.hostname||','||conf.datadir
              FROM gp_segment_configuration conf
              WHERE conf.content=0 AND conf.dbid=2;
            """
        else:
            sql = """
              SELECT conf.hostname||','||pgfse.fselocation
              FROM pg_filespace_entry pgfse, gp_segment_configuration conf
              WHERE pgfse.fsefsoid=3052 AND conf.dbid=pgfse.fsedbid
              AND conf.content=0 AND conf.dbid=2;
            """
    else:
        return 1, ["Invalid iflag: {}".format(iflag)]

    cmd = "PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c \"{}\" -d postgres".format(sql)
    print(cmd)
    proc = subprocess.Popen(["bash", "-c", cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    if proc.returncode:
        return 1, ["psql error ={}=".format(sql)]

    tmplist = [line for line in out.strip().split("\n") if line]
    return 0, tmplist


def run_on_segdir(run_cmd):
    ret, seg_dir_list = get_seg_dir(0)
    if ret:
        return ret

    if not seg_dir_list:
        print("No segment directories found.")
        return 1

    # Confirmation info
    cmdstr = seg_dir_list[0].strip().split(",")
    host = cmdstr[0].strip()
    datadir = cmdstr[1].strip()
    cmd = 'ssh {} "cd {}; {}"'.format(host, datadir, run_cmd)

    while True:
        print("Please Confirm Command: \n{}\n(Yy/Nn)".format(cmd))
        mychoice = input().strip()
        if mychoice in ("y", "Y"):
            break
        elif mychoice in ("n", "N"):
            sys.exit(0)

    for entry in seg_dir_list:
        cmdstr = entry.strip().split(",")
        host = cmdstr[0].strip()
        datadir = cmdstr[1].strip()
        cmd = 'ssh {} "cd {}; {}" 2>&1'.format(host, datadir, run_cmd)
        print("cmd[{}]".format(cmd))
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if hasattr(out, 'decode'):
            out = out.decode('utf-8', 'replace')
        if hasattr(err, 'decode'):
            err = err.decode('utf-8', 'replace')
        print(out, end="")

    return 0


def main():
    if len(sys.argv) != 2:
        print("Argument number Error\nExample:\npython {}  run_command".format(sys.argv[0]))
        sys.exit(1)

    run_cmd = sys.argv[1]
    ret = run_on_segdir(run_cmd)
    return ret


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
