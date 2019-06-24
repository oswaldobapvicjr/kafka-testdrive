rem ===============================================================
rem =                 KAFKA SERVER STARTUP UTILITY                =
rem ===============================================================
rem ----------------   author: oswaldo.bapvic.jr   ----------------
rem ----------------   version: 1.0 (2019-06-24)   ----------------
rem ===============================================================

rem Environment variables

set KAFKA_HOME=\Apps\kafka_2.12-2.2.1

set KAFKA_BIN=bin\windows
set ZOOKEEPER_CONFIG=config\zookeeper.properties
set KAFKASERVER_CONFIG=config\server.properties

cd %KAFKA_HOME%

rem Starting up server applications...

start "Zookeeper Server" %KAFKA_BIN%\zookeeper-server-start.bat %ZOOKEEPER_CONFIG%

timeout 5

start "Kafka Server" %KAFKA_BIN%\kafka-server-start.bat %KAFKASERVER_CONFIG%

rem Done
