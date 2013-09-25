@echo off
title QuickCached %*

rem set JAVA_HOME=d:\jdk1.6.0_23
rem set JAVA=%JAVA_HOME%\bin\java
rem set cp1=-cp %JAVA_HOME%\lib\tools.jar;.;

set JVM_OPTIONS=-XX:CompileThreshold=1500 -XX:+UseConcMarkSweepGC

rem %JAVA% %cp1% -server -Xms512m -Xmx512m %JVM_OPTIONS% -Dappname=QC1 -jar dist\QuickCached-Server.jar %*
java -server -Xms512m -Xmx512m %JVM_OPTIONS% -Dappname=QC1 -jar dist\QuickCached-Server.jar %*

