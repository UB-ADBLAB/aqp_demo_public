#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

N=3

if [ $# -lt 1 ]; then
    echo "usage: $0 <qno>"
    exit 1
fi

QNAME="q$1"

for suffix in swr bernoulli exact; do
    echo "Running $suffix"
    ./run_q_mult.sh ${QNAME}_${suffix}.sql $N
done


