@echo off
setlocal enabledelayedexpansion

if not exist bin mkdir bin

echo Compiling Java source files...
powershell -Command "$files = (Get-ChildItem -Path src -Recurse -Filter *.java).FullName; javac -d bin $files"
if errorlevel 1 (
    echo Compilation failed.
    exit /b 1
)

echo Starting BrokerServer (TCP: 9092, HTTP: 8080)...
java -cp bin broker.BrokerServer 9092 8080 ./kafka_data
