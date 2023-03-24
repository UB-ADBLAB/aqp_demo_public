Approximate Queries for Concurrent Updates (demo)
======================

# Usage

1. Build PostgreSQL and install it.
2. Build aqp_pgext. `pg_config` should be on path.
3. Run ./workload/makeJar.sh to build the TPC-C workload runner.
4. Generate data using dbgen and load data (table defs and index defs are in ./queries/).
5. Update aqp/aqpConfig.json
    - username: PG username
    - password: PG password
    - database: PG database name
    - host: PG hostname
    - port: PG port
    - aqpServerPort: frontend/backend port
    - txnDriverJar: the path to the jar created by ./workload/makeJar.sh. This should be
    an absolute path, or a relative path relative to demo_app/src/server/common.
    - aqpNativeSvcPort: port that abtree metrics reporter runs (see postgresql.conf, abtree_metrics_reporter_port)
    - sMemPath: path to smem(8). Note that smem only reports mem usages of the part of /proc
    that is accessibly by the current user. If PG is running in a different user,
    https://github.com/zzy7896321/runas might be helpful for creating an executable to
    run smem as the user running the PG.
    - clusterName: cluster_name in postgresql conf, used to grep ps and smem outputs
6. aqp/demoClientConfig.json contains the 6 example queries and a few workload mixes.
7. Run ./bin/setup_demo.sh.
8. Run ./bin/start_demo.sh to start frontend/backend server.
9. Run ./bin/stop_demo.sh to stop frontend/backend server.

# Directories

- pg13_abtree: the modified PostgreSQL 13 with AB-tree impl in VLDB '22.
- queries: the SQL queries in demo
- workload: the TPC-C workload and background sampler JDBC driver
- aqp_pgext: the pgAQP PostgreSQL extension
- demo_app: the frontend and backend of the demo
- dbgen: the TPC-C initial data generator based on the TPC-H code
     generator.  We also incorporated part of the TPC-H schema. Use -W to
     specify the number of warehouses and -D to specify how many days of
     orders are generated in the initial data (with roughly 1000 orders per
     day per warehouse-district).
- conf: sample configuration files for demo_app and PG
- bin: utilities for building, starting and stoping the frontend/backend

