#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

if [ $# -lt 2 ]; then
    echo "usage: $0 <sqlfile> <n>"
    exit 1
fi

N=$2

for ((i = 0; i < $N; ++i)); do
    ./run_q.sh "$1"
done
