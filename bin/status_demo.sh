#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

cd ../demo_app
PORT="`cat src/common/aqpConfig.json | jq '.["aqpServerPort"]'`"
PID="$(lsof -i:$PORT | tail -n 1 | awk '{ print $2; }')"

if [ -n "$PID" ]; then
    echo "demo server running at port $PORT (pid = $PID)"
else
    echo "demo server is not running (expecting port $PORT)"
fi
