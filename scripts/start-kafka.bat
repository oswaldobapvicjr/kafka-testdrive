@echo off

echo ===============================================================
echo =                 KAFKA SERVER STARTUP UTILITY                =
echo ===============================================================
echo ----------------   author: oswaldo.bapvic.jr   ----------------
echo ----------------   version: 1.0 (2019-06-24)   ----------------
echo ===============================================================
echo;

rem Environment variables

set KAFKA_HOME=\Apps\kafka_2.12-2.2.1

set KAFKA_BIN=bin\windows
set ZOOKEEPER_CONFIG=config\zookeeper.properties
set KAFKASERVER_CONFIG=config\server.properties

cd %KAFKA_HOME%

echo Starting up server applications...

start "Zookeeper Server" %KAFKA_BIN%\zookeeper-server-start.bat %ZOOKEEPER_CONFIG%

timeout 5

start "Kafka Server" %KAFKA_BIN%\kafka-server-start.bat %KAFKASERVER_CONFIG%

echo Done
