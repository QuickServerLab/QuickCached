#!/bin/bash

nohup java -server -Dappname=QC1 -Xms512m -Xmx512m -XX:CompileThreshold=1500 -XX:+UseConcMarkSweepGC -jar dist/QuickCached-Server.jar $@ &
