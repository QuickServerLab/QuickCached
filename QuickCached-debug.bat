@echo off
title QuickCached %*

rem set JAVA_HOME=d:\jdk1.6.0_23
rem set JAVA=%JAVA_HOME%\bin\java
rem set cp1=-cp %JAVA_HOME%\lib\tools.jar;.;

set GC_OPTIONS=-verbose:gc -Xloggc:verbose_gc.log -XX:+PrintGCDetails -XX:+PrintHeapAtGC -XX:+HeapDumpOnOutOfMemoryError -XX:+PrintClassHistogram -XX:+PrintCommandLineFlags -XX:+PrintConcurrentLocks -XX:+PrintGCTimeStamps 
rem -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution  

rem set QUICKCACHED_OPTS="-Xms512m -Xmx512m"
set JVM_OPTIONS=-XX:CompileThreshold=1500 -XX:+UseConcMarkSweepGC

rem %JAVA% %cp1% -server -Xms512m -Xmx512m %JVM_OPTIONS% %GC_OPTIONS% %EXTRA_OPTS% -jar dist\QuickCached-Server.jar %*
java -server %QUICKCACHED_OPTS% %JVM_OPTIONS% %GC_OPTIONS% %EXTRA_OPTS% -jar dist\QuickCached-Server.jar %*
