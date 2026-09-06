#!/usr/bin/env bash
set -e

mkdir -p bin
echo "Compiling Java source files..."
javac -d bin $(find src -name "*.java")

echo "Starting BrokerServer (TCP: 9092, HTTP: 8080)..."
java -cp bin broker.BrokerServer 9092 8080 ./kafka_data
