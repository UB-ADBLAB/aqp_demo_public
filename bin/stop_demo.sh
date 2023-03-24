#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

./status_demo.sh

cd ../demo_app
PORT="`cat src/common/aqpConfig.json | jq '.["aqpServerPort"]'`"
PID="$(lsof -i:$PORT | tail -n 1 | awk '{ print $2; }')"

kill -2 $PID

../bin/status_demo.sh

