#!/usr/bin/env python
from __future__ import print_function
"""Fork test: spawns child processes with concurrency control."""

import os
import signal
import subprocess
import sys
import time

num_proc = 0
num_finish = 0
mainpid = os.getpid()


def handler(signum, frame):
    """Handle SIGCHLD: reap finished child processes."""
    global num_proc, num_finish

    c_pid = os.getpid()
    print("current pid={0}=".format(c_pid))
    print("current num_proc={0}=".format(num_proc))
    if c_pid == mainpid:
        if num_proc == 0:
            return
        print("I'm main, received a child process exit signal")
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid <= 0:
                    break
                num_proc -= 1
                print("Retrieve a child process. num_proc={0}=".format(num_proc))
                num_finish += 1
            except ChildProcessError:
                break


signal.signal(signal.SIGCHLD, handler)


def main():
    global num_proc, num_finish

    if len(sys.argv) != 3:
        print("Argument number Error\nExample:\npython3 {0} concurrency totalrun".format(sys.argv[0]))
        sys.exit(1)

    concurrency = int(sys.argv[1])
    itotal = int(sys.argv[2])

    for icalc in range(itotal):
        pid = os.fork()

        if pid < 0:
            print("Can not fork a child process!!!\n{0}".format(os.strerror(pid)))
            sys.exit(-1)

        if pid == 0:
            # Child process
            childpid = os.getpid()
            print("I'm a child process, pid={0}=".format(childpid))
            if childpid % 5 == 0:
                time.sleep(13)
                print("I'm a child process, pid={0}=. I will exit -1".format(childpid))
                os._exit(255)
            elif childpid % 7 == 0:
                devnull = open(os.devnull, 'w')
                proc = subprocess.Popen(
                    ["psql", "-d", "testdb", "-ac",
                     "select count(*) from to_cdr_partname_varchar_spec33;"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                proc.communicate()
                devnull.close()
                print("I'm a child process, pid={0}=. I run psql. I will exit 0".format(childpid))
                os._exit(255)
            elif childpid % 9 == 0:
                proc = subprocess.Popen(
                    ["gpcheck", os.path.expanduser("~/allhsots")],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                proc.communicate()
                print("I'm a child process, pid={0}=. I run gpcheck. I will exit 0".format(childpid))
                os._exit(255)
            else:
                time.sleep(9)
                print("I'm a child process, pid={0}=. I will exit 0".format(childpid))
                os._exit(0)

        else:
            # Parent process
            num_proc += 1
            if num_finish % 10 == 0:
                print("Child process count [{0}], finish count[{1}/{2}]".format(num_proc, num_finish, itotal))
            while num_proc >= concurrency:
                time.sleep(1)

    print("waiting for all child finished!")
    while True:
        try:
            ichd, _ = os.waitpid(-1, os.WNOHANG)
            if ichd > 0:
                num_finish += 1
            else:
                time.sleep(1)
        except ChildProcessError:
            break

    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
