#!/bin/bash

if [ $# -lt 2 ]; then
    echo "usage: $0 <nTPCCThreads> <nSamplingThreads>"
    exit 1
fi

 ./run $1 $2 arches.cse.buffalo.edu 7890 tpcc_w100_cwang39 cwang39 JthbuL4d3h "" "0.5,0.4,0,0.1,0" 10000
 


