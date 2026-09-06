# Manual Testing & Operational Guide

This document provides step-by-step instructions for compiling, executing, and testing the message broker using terminal commands, HTTP endpoints, client code, and the web interface.

---

## Table of Contents
1. [Prerequisites](#1-prerequisites)
2. [Step 1: Compiling from Source](#step-1-compiling-from-source)
3. [Step 2: Starting the Broker Server](#step-2-starting-the-broker-server)
4. [Step 3: Running the Automated Integration Test](#step-3-running-the-automated-integration-test)
5. [Step 4: Manual Testing via the Web Dashboard](#step-4-manual-testing-via-the-web-dashboard)
6. [Step 5: Manual Testing via Terminal / HTTP REST API](#step-5-manual-testing-via-terminal--http-rest-api)
7. [Step 6: Manual Testing via Java Client Code](#step-6-manual-testing-via-java-client-code)
8. [Step 7: Manual Crash & Recovery Test](#step-7-manual-crash--recovery-test)
9. [Component Architecture Reference](#component-architecture-reference)

---

## 1. Prerequisites
- **Java SE 21+** installed (`java -version` and `javac -version`).
- No external libraries or build tools required.

---

## Step 1: Compiling from Source

Navigate to the project root directory:
```powershell
cd kafka-clone
```

Create the output directory:
```powershell
if (!(Test-Path "bin")) { New-Item -ItemType Directory -Path "bin" }
```

Compile all Java source files:
```powershell
$files = (Get-ChildItem -Path src -Recurse -Filter *.java).FullName
javac -d bin $files
```

*(On Linux / macOS: `javac -d bin $(find src -name "*.java")`)*

---

## Step 2: Starting the Broker Server

Using the startup scripts:
- **Windows**: `start.bat`
- **Linux / macOS**: `./start.sh`

Or run directly via Java:
```powershell
java -cp bin broker.BrokerServer 9092 8080 ./kafka_data
```

### Parameter Reference:
- `9092`: TCP port for wire protocol clients (`Producer` and `Consumer`).
- `8080`: HTTP port for web dashboard and REST API.
- `./kafka_data`: Storage directory on disk where log segments and index files are written.

Console output confirms initialization:
```text
=================================================
   KAFKA CLONE BROKER RUNNING
   TCP Wire Port:  9092
   Web Dashboard:  http://localhost:8080
   Data Directory: ./kafka_data
=================================================
```

---

## Step 3: Running the Automated Integration Test

Open a second terminal window and execute:
```powershell
cd kafka-clone
java -cp bin test.BrokerTest
```

### Test Scope:
1. Spawns an isolated test broker on ephemeral ports.
2. Produces 20 test messages over TCP with monotonic offsets `0..19`.
3. Consumes all 20 records and verifies payload data and CRC32 checksums.
4. Commits a consumer group offset (`billing-service-group`).
5. Queries and confirms the committed offset.
6. Terminates and restarts the broker against the same data directory.
7. Replays historical messages from disk and verifies consumer group offset persistence.

---

## Step 4: Manual Testing via the Web Dashboard

Open `http://localhost:8080` in a browser.

### Test 1: Create a Topic
1. Click the `+ NEW TOPIC` button in the **Topic Registry** panel.
2. Enter Topic Name: `payments`.
3. Set Partition Count: `3`.
4. Click `CREATE TOPIC`.
5. Verify that `# payments` appears with 3 partitions in the topic list.

### Test 2: Produce Messages
1. In the **Message Producer Studio**:
   - Select `payments` as the topic and `Partition 0`.
   - Click the `Order JSON` template chip to populate sample payload data.
   - Click `SEND TO BROKER (O(1) DISK APPEND)`.
2. Inspect the **Last Append Receipt**:
   - `OFFSET`: Assigned sequence number (e.g. `#0`).
   - `CRC32`: Calculated data integrity checksum.
   - `TIMESTAMP`: Append timestamp and latency in milliseconds.

### Test 3: Consume Messages
1. In the **Consumer Group Playground**:
   - Set Group ID: `payment-audit`.
   - Select Topic: `payments` and Partition: `0`.
   - Click `POLL (NEXT 10)`.
2. Verify:
   - The message appears in the **Poll Buffer** list.
   - The consumer position indicator advances (`POS: 1`).

### Test 4: Commit Consumer Offset
1. Click `COMMIT OFFSET`.
2. The button reflects confirmation: `COMMITTED (OFFSET 1)`.
3. The broker records `payment-audit:payments:0=1` in `kafka_data/__consumer_offsets.dat`.

### Test 5: Replay Historical Records (Seek)
1. In the Consumer panel, set `SEEK OFFSET` to `0` and click `SEEK`.
2. The consumer position indicator resets to `POS: 0`.
3. Click `POLL (NEXT 10)`. All past messages are returned. This verifies that consumption does not delete records from the commit log.

### Test 6: Inspect Storage on Disk
1. Click the `DISK LOG & INDEX INSPECTOR` tab.
2. Select Topic `payments` and click `INSPECT FILES`.
3. Verify:
   - Absolute partition directory path.
   - Active segment entries (`00000000000000000000.log` and `00000000000000000000.index`).
   - Physical byte sizes and indexed entry counts.
   - Parsed byte layout previews.

---

## Step 5: Manual Testing via Terminal / HTTP REST API

The broker exposes standard HTTP endpoints:

### 1. Check Broker Status
```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/status
```

### 2. Create Topic
```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/create-topic -Method Post -ContentType "application/json" -Body '{"topic":"iot-sensors","partitions":2}'
```

### 3. Produce Message
```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/produce -Method Post -ContentType "application/json" -Body '{"topic":"iot-sensors","partition":0,"key":"sensor-1","value":"temp: 24.5C"}'
```
Response:
```json
{"success":true,"topic":"iot-sensors","partition":0,"offset":0,"timestamp":1788686348154,"crc":1001826422}
```

### 4. Fetch Messages
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/api/consume?topic=iot-sensors&partition=0&offset=0&limit=5"
```

### 5. Commit Offset
```powershell
Invoke-RestMethod -Uri http://localhost:8080/api/commit -Method Post -ContentType "application/json" -Body '{"groupId":"sensor-readers","topic":"iot-sensors","partition":0,"offset":1}'
```

---

## Step 6: Manual Testing via Java Client Code

Standard Java client implementation examples over TCP port `9092`:

### Producer Example (`Producer.java`)
```java
import client.Producer;
import client.Producer.RecordMetadata;

public class TestProducer {
    public static void main(String[] args) throws Exception {
        try (Producer producer = new Producer("127.0.0.1", 9092)) {
            RecordMetadata meta = producer.send("orders", "user_101", "Order Placed: $150.00");
            System.out.println("Appended at offset " + meta.offset + " in partition " + meta.partition);
        }
    }
}
```

### Consumer Example (`Consumer.java`)
```java
import client.Consumer;
import model.Message;
import java.util.List;

public class TestConsumer {
    public static void main(String[] args) throws Exception {
        try (Consumer consumer = new Consumer("127.0.0.1", 9092)) {
            consumer.subscribe("orders", 0);
            
            // Resume from last committed offset
            consumer.loadAndSeekCommittedOffset("order-workers");

            List<Message> records = consumer.poll(10, 3000);
            for (Message m : records) {
                System.out.println("Consumed offset " + m.getOffset() + ": " + m.getValueAsString());
            }

            // Commit position to broker
            consumer.commitSync("order-workers");
        }
    }
}
```

---

## Step 7: Manual Crash & Recovery Test

Verifies commit log durability across ungraceful process terminations:

1. **Produce 10 messages** into topic `orders` partition `0`.
2. **Commit offset 10** for consumer group `finance-group`.
3. **Terminate the broker process**:
   - In the terminal running `BrokerServer`, send `SIGINT` (`Ctrl + C`).
4. **Inspect the data directory**:
   - Check `kafka_data/orders/partition-0/`.
   - Verify `.log` and `.index` files exist on disk.
   - Check `kafka_data/__consumer_offsets.dat` to confirm `finance-group:orders:0=10`.
5. **Restart the broker**:
   ```powershell
   java -cp bin broker.BrokerServer 9092 8080 ./kafka_data
   ```
6. **Verify recovery**:
   - The broker reconstructs segments and restores partition high-watermarks to `10`.
   - Polling from offset `0` replays all 10 messages without loss.
   - Querying `finance-group` confirms the committed offset survived intact.

---

## Component Architecture Reference

| Component | File Path | Technical Responsibility |
| :--- | :--- | :--- |
| **`Message.java`** | `src/model/Message.java` | Wire and storage record model: 8-byte Offset, 8-byte Timestamp, Key, Value, and CRC32 verification. |
| **`Protocol.java`** | `src/model/Protocol.java` | Length-prefixed framing (`[Length][Payload]`), request/response headers, error codes, and binary codec routines. |
| **`CommitLog.java`** | `src/storage/CommitLog.java` | Sequential append-only commit log engine. Handles NIO FileChannel writes, segment rolling, and startup crash recovery. |
| **`OffsetIndex.java`** | `src/storage/OffsetIndex.java` | Fixed 16-byte index entries (`[8B offset \| 8B file position]`). Provides O(log N) binary floor lookups. |
| **`TopicRegistry.java`** | `src/broker/TopicRegistry.java` | Topic and partition coordinator. Manages consumer group offset commits with atomic disk persistence. |
| **`BrokerServer.java`** | `src/broker/BrokerServer.java` | Central multi-threaded server. Manages TCP socket listener on port 9092 and embedded HTTP server on port 8080. |
| **`Producer.java`** | `src/client/Producer.java` | Thread-safe producer client with key-based partition routing. |
| **`Consumer.java`** | `src/client/Consumer.java` | Sequential polling consumer client with offset advancement and commit operations. |
| **`BrokerTest.java`** | `src/test/BrokerTest.java` | Integration and reboot recovery test suite. |
| **Web Dashboard** | `web/index.html, style.css, app.js` | Interactive web dashboard and visual architecture pipeline viewer. |
