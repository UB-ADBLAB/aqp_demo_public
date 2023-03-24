#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

./status_demo.sh

cd ../demo_app
mkdir -p ../log
nohup npm run serv &> ../log/node_log_`date +%s`.log &

sleep 1
../bin/status_demo.sh

