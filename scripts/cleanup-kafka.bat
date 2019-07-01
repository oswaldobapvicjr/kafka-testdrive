@echo off

echo ===============================================================
echo =                 KAFKA SERVER CLEANUP UTILITY                =
echo ===============================================================
echo ----------------   author: oswaldo.bapvic.jr   ----------------
echo ----------------   version: 1.0 (2019-07-01)   ----------------
echo ===============================================================
echo;

rem Environment variables

set KAFKA_LOGS_PATH=\tmp\kafka-logs
set ZOOKEEPER_LOGS_PATH=\tmp\zookeeper

echo WARNING! This will delete all data from Kafka topics.
set /P confirmation=[Are you sure? (Y/N)]

if /I %confirmation% NEQ Y goto :EOF

echo Cleaning-up Kafka logs...
rmdir /S /Q %KAFKA_LOGS_PATH%

echo;
echo Cleaning-up Zookeeper logs...
rmdir /S /Q %ZOOKEEPER_LOGS_PATH%

echo Done
