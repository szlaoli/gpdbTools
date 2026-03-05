#!/usr/bin/env python3
import subprocess
import sys
from datetime import datetime


def get_current_datetime() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def get_gpver() -> str:
    sql = "select version();"
    result = subprocess.run(
        ["bash", "-c", f"PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c \"{sql}\" -d postgres"],
        capture_output=True, text=True
    )
    if result.returncode:
        print("Get GP version error!")
        sys.exit(1)
    sver = result.stdout.strip()
    tmpstr = sver.split(" ")
    print(tmpstr[4])
    tmpver = tmpstr[4].split(".")
    print(tmpver[0])
    return tmpver[0]


def get_seg_dir(iflag: int) -> tuple[int, list[str]]:
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
        return 1, [f"Invalid iflag: {iflag}"]

    cmd = f"PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c \"{sql}\" -d postgres"
    print(cmd)
    result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
    if result.returncode:
        return 1, [f"psql error ={sql}="]

    tmplist = [line for line in result.stdout.strip().split("\n") if line]
    return 0, tmplist


def run_on_segdir(run_cmd: str) -> int:
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
    cmd = f'ssh {host} "cd {datadir}; {run_cmd}"'

    while True:
        print(f"Please Confirm Command: \n{cmd}\n(Yy/Nn)")
        mychoice = input().strip()
        if mychoice in ("y", "Y"):
            break
        elif mychoice in ("n", "N"):
            sys.exit(0)

    for entry in seg_dir_list:
        cmdstr = entry.strip().split(",")
        host = cmdstr[0].strip()
        datadir = cmdstr[1].strip()
        cmd = f'ssh {host} "cd {datadir}; {run_cmd}" 2>&1'
        print(f"cmd[{cmd}]")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result.stdout, end="")

    return 0


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Argument number Error\nExample:\npython3 {sys.argv[0]} run_command")
        sys.exit(1)

    run_cmd = sys.argv[1]
    ret = run_on_segdir(run_cmd)
    return ret


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
