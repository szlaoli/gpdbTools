#!/usr/bin/env python3
"""Fork test: spawns child processes with concurrency control."""

import os
import signal
import subprocess
import sys
import time

num_proc: int = 0
num_finish: int = 0
mainpid: int = os.getpid()


def handler(signum: int, frame: object) -> None:
    """Handle SIGCHLD: reap finished child processes."""
    global num_proc, num_finish

    c_pid = os.getpid()
    print(f"current pid={c_pid}=")
    print(f"current num_proc={num_proc}=")
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
                print(f"Retrieve a child process. num_proc={num_proc}=")
                num_finish += 1
            except ChildProcessError:
                break


signal.signal(signal.SIGCHLD, handler)


def main() -> int:
    global num_proc, num_finish

    if len(sys.argv) != 3:
        print(f"Argument number Error\nExample:\npython3 {sys.argv[0]} concurrency totalrun")
        sys.exit(1)

    concurrency: int = int(sys.argv[1])
    itotal: int = int(sys.argv[2])

    for icalc in range(itotal):
        pid = os.fork()

        if pid < 0:
            print(f"Can not fork a child process!!!\n{os.strerror(pid)}")
            sys.exit(-1)

        if pid == 0:
            # Child process
            childpid = os.getpid()
            print(f"I'm a child process, pid={childpid}=")
            if childpid % 5 == 0:
                time.sleep(13)
                print(f"I'm a child process, pid={childpid}=. I will exit -1")
                os._exit(255)
            elif childpid % 7 == 0:
                subprocess.run(
                    ["psql", "-d", "testdb", "-ac",
                     "select count(*) from to_cdr_partname_varchar_spec33;"],
                    check=False,
                )
                print(f"I'm a child process, pid={childpid}=. I run psql. I will exit 0")
                os._exit(255)
            elif childpid % 9 == 0:
                subprocess.run(["gpcheck", os.path.expanduser("~/allhsots")], check=False)
                print(f"I'm a child process, pid={childpid}=. I run gpcheck. I will exit 0")
                os._exit(255)
            else:
                time.sleep(9)
                print(f"I'm a child process, pid={childpid}=. I will exit 0")
                os._exit(0)

        else:
            # Parent process
            num_proc += 1
            if num_finish % 10 == 0:
                print(f"Child process count [{num_proc}], finish count[{num_finish}/{itotal}]")
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
