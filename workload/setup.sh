#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

./makeJar.sh
cd sql
PGPASSWORD="JthbuL4d3h" psql -h arches.cse.buffalo.edu -p 7890 -U cwang39 tpcc_w100_cwang39 -f load_all.sql
cd ..

