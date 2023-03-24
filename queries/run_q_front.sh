#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: $0 <sqlfile>"
    exit 1
fi

echo '\timing on' | cat - "$1" |\
PGPASSWORD="+zaZawb4=" psql -h arches.cse.buffalo.edu -p 7890 -U frontend tpcc_w100_front

