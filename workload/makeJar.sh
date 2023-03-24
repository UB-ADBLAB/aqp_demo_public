#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

./compile || exit 1
cd bin
jar -xf ../external/postgresql-42.5.4.jar
mkdir -p META-INF/licenses/org.postgresql
mv META-INF/LICENSE META-INF/licenses/org.postgresql/
rm -f META-INF/MANIFEST.MF
jar cfe ../htapaqp.jar edu.buffalo.htapaqp.HTAPAQPDriver *


