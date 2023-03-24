#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: $0 <sqlfile>"
    exit 1
fi

echo '\timing on' | cat - "$1" |\
PGPASSWORD="JthbuL4d3h" psql -h arches.cse.buffalo.edu -p 7890 -U cwang39 tpcc_w100_cwang39

