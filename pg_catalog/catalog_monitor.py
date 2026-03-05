#!/usr/bin/env python
from __future__ import print_function
import os
import subprocess
import sys


hostname = "localhost"
port = "5432"
database = ""
username = ""
gpver = ""


def set_env():
    os.environ["PGHOST"] = "localhost"
    os.environ["PGDATABASE"] = database
    os.environ["PGUSER"] = username
    os.environ["PGPASSWORD"] = ""


def run_psql(sql, extra_args=None):
    args = ["psql", "-A", "-X", "-t", "-c", sql]
    if extra_args:
        args.extend(extra_args)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return proc.returncode >> 8 if proc.returncode > 255 else proc.returncode, out.strip()


def run_psql_utility(sql, quiet=False):
    cmd_args = ["psql", "-A", "-X", "-t", "-c", sql]
    if quiet:
        cmd_args.insert(3, "-q")
    env = dict(os.environ)
    env["PGOPTIONS"] = "-c gp_session_role=utility"
    proc = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    if hasattr(err, 'decode'):
        err = err.decode('utf-8', 'replace')
    return proc.returncode, out.strip()


def get_gpver():
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


def catalog_history():
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
    print("---{}---pg_catalog info---".format(database))
    print("pg_tables count:                     {}".format(table_count))
    print("pg_views count:                      {}".format(view_count))
    print("pg_namespace count:                  {}".format(pg_namespace_count))
    print("pg_namespace size:                   {}".format(pg_namespace_size))
    print("pg_namespace size in master:         {}".format(pg_namespace_master))
    print("pg_namespace size in gpseg0:         {}".format(pg_namespace_gpseg0))
    print("pg_namespace bloat in master:        {}".format(pg_namespace_master_bloat))
    print("pg_class count:                      {}".format(pg_class_count))
    print("pg_class size:                       {}".format(pg_class_size))
    print("pg_class size in master:             {}".format(pg_class_master))
    print("pg_class size in gpseg0:             {}".format(pg_class_gpseg0))
    print("pg_class bloat in master:            {}".format(pg_class_master_bloat))
    print("pg_attribute count:                  {}".format(pg_attribute_count))
    print("pg_attribute size:                   {}".format(pg_attribute_size))
    print("pg_attribute size in master:         {}".format(pg_attribute_master))
    print("pg_attribute size in gpseg0:         {}".format(pg_attribute_gpseg0))
    print("pg_attribute bloat in master:        {}".format(pg_attribute_master_bloat))
    print("pg_partition_rule count:             {}".format(pg_partition_rule_count))
    print("pg_partition_rule size in master:    {}".format(pg_partition_rule_size))
    print("pg_partition_rule bloat in master:   {}".format(pg_partition_rule_bloat))
    print("pg_statistic count:                  {}".format(pg_statistic_count))
    print("pg_statistic size in master:         {}".format(pg_statistic_size))
    print("")

    return 0


def main():
    global database, username, gpver

    if len(sys.argv) != 2:
        print("Argument number Error\nExample:\npython {} dbname".format(sys.argv[0]))
        sys.exit(1)

    database = sys.argv[1]
    proc = subprocess.Popen(["whoami"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if hasattr(out, 'decode'):
        out = out.decode('utf-8', 'replace')
    username = out.strip()

    set_env()
    gpver = get_gpver()
    catalog_history()

    return 0


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
