#!/bin/bash
#-XX:+UseParallelGC
#export QUICKCACHED_OPTS="-Xms512m -Xmx512m"
exec java -server -Dappname=QC1 $QUICKCACHED_OPTS -XX:CompileThreshold=1500 -XX:+UseConcMarkSweepGC -jar dist/QuickCached-Server.jar $@
