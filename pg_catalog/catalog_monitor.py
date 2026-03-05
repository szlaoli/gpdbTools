#!/usr/bin/env python3
import os
import subprocess
import sys


hostname: str = "localhost"
port: str = "5432"
database: str = ""
username: str = ""
gpver: str = ""


def set_env() -> None:
    os.environ["PGHOST"] = "localhost"
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = ""


def run_psql(sql: str, extra_args: list[str] | None = None) -> tuple[int, str]:
    args = ["psql", "-A", "-X", "-t", "-c", sql]
    if extra_args:
        args.extend(extra_args)
    result = subprocess.run(args, capture_output=True, text=True)
    return result.returncode >> 8 if result.returncode > 255 else result.returncode, result.stdout.strip()


def run_psql_utility(sql: str, quiet: bool = False) -> tuple[int, str]:
    cmd_args = ["psql", "-A", "-X", "-t", "-c", sql]
    if quiet:
        cmd_args.insert(3, "-q")
    env = dict(os.environ)
    env["PGOPTIONS"] = "-c gp_session_role=utility"
    result = subprocess.run(cmd_args, capture_output=True, text=True, env=env)
    return result.returncode, result.stdout.strip()


def get_gpver() -> str:
    sql = "select version();"
    ret, sver = run_psql(sql, ["-d", "postgres"])
    if ret:
        print("Get GP version error!")
        sys.exit(1)
    tmpstr = sver.split(" ")
    print(tmpstr[4])
    tmpver = tmpstr[4].split(".")
    print(tmpver[0])
    return tmpver[0]


def catalog_history() -> int:
    # pg_tables count
    ret, table_count = run_psql("select count(*) from pg_tables;")
    if ret:
        print("pg_tables count error!")
        return -1

    # pg_views count
    ret, view_count = run_psql("select count(*) from pg_views;")
    if ret:
        print("pg_views count error!")
        return -1

    # pg_namespace
    ret, pg_namespace_size = run_psql("select pg_relation_size('pg_namespace');")
    if ret:
        print("pg_namespace size error!")
        return -1

    ret, pg_namespace_master = run_psql_utility("select pg_relation_size('pg_namespace');")
    if ret:
        print("pg_namespace master size error!")
        return -1

    ret, pg_namespace_gpseg0 = run_psql(
        "select pg_relation_size('pg_namespace') from gp_dist_random('gp_id') where gp_segment_id=0;"
    )
    if ret:
        print("pg_namespace gpseg0 size error!")
        return -1

    sql = ("create temp table tmp_pg_namespace_record as select * from pg_namespace distributed randomly;\n"
           "select pg_relation_size('tmp_pg_namespace_record');")
    ret, pg_namespace_realsize = run_psql_utility(sql, quiet=True)
    if ret:
        print("pg_namespace realsize error!")
        return -1

    pg_namespace_master_bloat = int(pg_namespace_master) / int(pg_namespace_realsize) if int(pg_namespace_realsize) else 0

    ret, pg_namespace_count = run_psql("select count(*) from pg_namespace;")
    if ret:
        print("pg_namespace count error!")
        return -1

    # pg_class
    ret, pg_class_size = run_psql("select pg_relation_size('pg_class');")
    if ret:
        print("pg_class size error!")
        return -1

    ret, pg_class_master = run_psql_utility("select pg_relation_size('pg_class');")
    if ret:
        print("pg_class master size error!")
        return -1

    ret, pg_class_gpseg0 = run_psql(
        "select pg_relation_size('pg_class') from gp_dist_random('gp_id') where gp_segment_id=0;"
    )
    if ret:
        print("pg_class gpseg0 size error!")
        return -1

    sql = ("create temp table tmp_pg_class_record as select * from pg_class distributed randomly;\n"
           "select pg_relation_size('tmp_pg_class_record');")
    ret, pg_class_realsize = run_psql_utility(sql, quiet=True)
    if ret:
        print("pg_class realsize error!")
        return -1

    pg_class_master_bloat = int(pg_class_master) / int(pg_class_realsize) if int(pg_class_realsize) else 0

    ret, pg_class_count = run_psql("select count(*) from pg_class;")
    if ret:
        print("pg_class count error!")
        return -1

    # pg_attribute
    ret, pg_attribute_size = run_psql("select pg_relation_size('pg_attribute');")
    if ret:
        print("pg_attribute size error!")
        return -1

    ret, pg_attribute_master = run_psql_utility("select pg_relation_size('pg_attribute');")
    if ret:
        print("pg_attribute master size error!")
        return -1

    ret, pg_attribute_gpseg0 = run_psql(
        "select pg_relation_size('pg_attribute') from gp_dist_random('gp_id') where gp_segment_id=0;"
    )
    if ret:
        print("pg_attribute gpseg0 size error!")
        return -1

    sql = ("create temp table tmp_pg_attribute_record as select * from pg_attribute distributed randomly;\n"
           "select pg_relation_size('tmp_pg_attribute_record');")
    ret, pg_attribute_realsize = run_psql_utility(sql, quiet=True)
    if ret:
        print("pg_attribute realsize error!")
        return -1

    pg_attribute_master_bloat = int(pg_attribute_master) / int(pg_attribute_realsize) if int(pg_attribute_realsize) else 0

    ret, pg_attribute_count = run_psql("select count(*) from pg_attribute;")
    if ret:
        print("pg_attribute count error!")
        return -1

    # pg_partition_rule
    ret, pg_partition_rule_size = run_psql("select pg_relation_size('pg_partition_rule');")
    if ret:
        print("pg_partition_rule size error!")
        return -1

    sql = ("create temp table tmp_pg_partition_rule_record as select * from pg_partition_rule distributed randomly;\n"
           "select pg_relation_size('tmp_pg_partition_rule_record');")
    ret, pg_partition_rule_realsize = run_psql_utility(sql, quiet=True)
    if ret:
        print("pg_partition_rule realsize error!")
        return -1

    pg_partition_rule_bloat = int(pg_partition_rule_size) / int(pg_partition_rule_realsize) if int(pg_partition_rule_realsize) else 0

    ret, pg_partition_rule_count = run_psql("select count(*) from pg_partition_rule;")
    if ret:
        print("pg_partition_rule count error!")
        return -1

    # pg_statistic
    ret, pg_statistic_size = run_psql("select pg_relation_size('pg_statistic');")
    if ret:
        print("pg_statistic size error!")
        return -1

    ret, pg_statistic_count = run_psql("select count(*) from pg_statistic;")
    if ret:
        print("pg_statistic count error!")
        return -1

    # Print results
    print(f"---{database}---pg_catalog info---")
    print(f"pg_tables count:                     {table_count}")
    print(f"pg_views count:                      {view_count}")
    print(f"pg_namespace count:                  {pg_namespace_count}")
    print(f"pg_namespace size:                   {pg_namespace_size}")
    print(f"pg_namespace size in master:         {pg_namespace_master}")
    print(f"pg_namespace size in gpseg0:         {pg_namespace_gpseg0}")
    print(f"pg_namespace bloat in master:        {pg_namespace_master_bloat}")
    print(f"pg_class count:                      {pg_class_count}")
    print(f"pg_class size:                       {pg_class_size}")
    print(f"pg_class size in master:             {pg_class_master}")
    print(f"pg_class size in gpseg0:             {pg_class_gpseg0}")
    print(f"pg_class bloat in master:            {pg_class_master_bloat}")
    print(f"pg_attribute count:                  {pg_attribute_count}")
    print(f"pg_attribute size:                   {pg_attribute_size}")
    print(f"pg_attribute size in master:         {pg_attribute_master}")
    print(f"pg_attribute size in gpseg0:         {pg_attribute_gpseg0}")
    print(f"pg_attribute bloat in master:        {pg_attribute_master_bloat}")
    print(f"pg_partition_rule count:             {pg_partition_rule_count}")
    print(f"pg_partition_rule size in master:    {pg_partition_rule_size}")
    print(f"pg_partition_rule bloat in master:   {pg_partition_rule_bloat}")
    print(f"pg_statistic count:                  {pg_statistic_count}")
    print(f"pg_statistic size in master:         {pg_statistic_size}")
    print()

    return 0


def main() -> int:
    global database, username, gpver

    if len(sys.argv) != 2:
        print(f"Argument number Error\nExample:\npython3 {sys.argv[0]} dbname")
        sys.exit(1)

    database = sys.argv[1]
    username = subprocess.run(["whoami"], capture_output=True, text=True).stdout.strip()

    set_env()
    gpver = get_gpver()
    catalog_history()

    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
