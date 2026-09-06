# Kafka Clone

A lightweight distributed message broker implementation in Java, designed to demonstrate the core architecture of Apache Kafka from first principles.

Key architectural features:
- **Append-Only Commit Log Storage Engine**: Sequential disk writes using Java NIO FileChannel.
- **Binary Offset Indexing**: Sparse/dense binary index files (.index) providing O(log N) floor offset lookups.
- **Binary Wire Protocol**: Length-prefixed binary wire framing over persistent TCP sockets.
- **Consumer Group Offset Management**: Server-side committed offset persistence (__consumer_offsets.dat).
- **Crash Recovery**: Automatic log segment scanning on startup to restore partition high-watermarks.
- **Web Dashboard & Architecture Explorer**: Built-in HTTP dashboard and REST API with zero external dependencies.

---

## Project Architecture

```
kafka-clone/
├── .gitignore                          <-- Git ignore configuration
├── start.sh                            <-- Unix / macOS startup script
├── start.bat                           <-- Windows startup script
├── README.md                           <-- Project overview and quickstart
├── MANUAL_TESTING_GUIDE.md             <-- Step-by-step testing instructions
├── docs/
│   └── ARCHITECTURE_AND_PIPELINE.md   <-- Storage engine & pipeline technical details
├── src/
│   ├── model/
│   │   ├── Message.java                <-- Binary message definition and CRC32 verification
│   │   └── Protocol.java               <-- Wire framing, request/response models, serialization
│   ├── storage/
│   │   ├── OffsetIndex.java            <-- Binary index file (.index) mapping offset to position
│   │   └── CommitLog.java              <-- Segmented commit log (.log), rolling, and recovery
│   ├── broker/
│   │   ├── TopicRegistry.java          <-- Topic/partition state and consumer group persistence
│   │   └── BrokerServer.java           <-- Multi-threaded TCP broker and embedded HTTP server
│   ├── client/
│   │   ├── Producer.java               <-- Key-hashing TCP producer client
│   │   └── Consumer.java               <-- Polling TCP consumer client with offset management
│   └── test/
│       └── BrokerTest.java             <-- Automated integration and recovery test suite
└── web/
    ├── index.html                      <-- Web dashboard structure
    ├── style.css                       <-- Dashboard design system
    └── app.js                          <-- Dashboard logic and pipeline explorer
```

---

## Quickstart

### Starting the Broker & Dashboard

#### On Linux / macOS / WSL:
```bash
chmod +x start.sh
./start.sh
```

#### On Windows:
```cmd
start.bat
```

#### Or using manual commands:
```powershell
$files = (Get-ChildItem -Path src -Recurse -Filter *.java).FullName
javac -d bin $files
java -cp bin broker.BrokerServer 9092 8080 ./kafka_data
```

Once running:
- **TCP Broker**: `localhost:9092`
- **Web Dashboard**: `http://localhost:8080`

### Running the Integration Test Suite
```powershell
java -cp bin test.BrokerTest
```

For complete manual testing workflows (curl, PowerShell, custom Java programs, crash recovery), refer to [MANUAL_TESTING_GUIDE.md](./MANUAL_TESTING_GUIDE.md).

---

## Web Dashboard & Component Guide

The web dashboard at `http://localhost:8080` provides real-time visualization and control over the message broker:

### 1. Cluster Status
- **BROKER STATUS**: Indicates whether the server is actively accepting connections.
- **TCP PORT (9092)**: The binary socket port for Java Producer and Consumer client connections.
- **WEB PORT (8080)**: The port serving the dashboard and REST API.
- **TOTAL MSGS**: Total count of all messages persisted across all topics and partitions.
- **UPTIME**: Server elapsed run time.
- **REFRESH STATE**: Triggers a poll of the broker's current metadata.

---

### 2. Live Broker Studio

#### Topic Registry
- **Topic**: A named stream of records (e.g. `orders`, `payments`).
- **Partition**: An ordered, immutable sequence of records continuously appended to a structured commit log. Topics are divided into partitions for parallel throughput.
- **High Watermark**: The highest offset in a partition, indicating where the next message will be appended.
- **+ NEW TOPIC**: Creates a new topic with a user-specified partition count.
- **Topic Cards**: Selecting a card updates the active target for producing and consuming.

#### Message Producer Studio
- **Topic & Partition Selectors**: Target selection for new messages.
- **Message Key**: Optional identifier. Messages sharing the same key hash to the same partition, preserving per-entity order.
- **Payload Value**: The message body (text or JSON).
- **Template Chips (Order JSON, User Event, Payment)**: Populates realistic test payloads.
- **SEND TO BROKER**:
  - Encapsulates the message into a length-prefixed binary TCP frame.
  - Sequentially appends the payload to the active `.log` file on disk via `FileChannel`.
  - Records the offset-to-file-position mapping in `.index`.
  - Displays the assigned offset, partition, CRC32 checksum, and round-trip latency.

#### Consumer Group Playground
- **Group ID**: The consumer group identifier. The broker coordinates offsets per group, allowing consumer instances to track read progress.
- **SEEK OFFSET & SEEK**: Rewinds or fast-forwards the consumer's local read pointer. Because Kafka logs are non-destructive, historical data can be replayed at any time.
- **POLL (NEXT 10)**:
  - Fetches unread records starting from the consumer's current offset pointer.
  - Recalculates and verifies CRC32 checksums for data integrity.
  - Appends records to the display buffer and advances the offset pointer.
- **COMMIT OFFSET**:
  - Persists the current processed offset to `__consumer_offsets.dat`.
  - Ensures consumer progress survives service restarts.
- **AUTO: OFF / ON**: Runs an automatic poll loop every 1.2 seconds to simulate continuous consumption.

---

### 3. Disk Log & Index Inspector
Exposes the physical storage engine on disk (`./kafka_data/`):
- **Base Offset**: The lowest offset contained in that log segment.
- **Log File (.log)**: The sequential binary append-only commit log file.
- **Index File (.index)**: Binary index entries mapping 64-bit offsets to file byte positions.
- **File Size**: Byte size of the segment on disk.
- **Indexed Entries**: Total number of indexed lookup points.
- **Status**: Distinguishes between the active writable segment and rolled read-only segments.
- **Raw Byte Layout Preview**: Displays individual message records parsed directly from disk.

---

### 4. Architecture & Pipeline Explorer
Provides visual representations and Java code previews for core execution paths:
- **1. Produce Flow**: Message serialization, TCP framing, partition write lock, monotonic offset assignment, NIO FileChannel append, and acknowledgment.
- **2. Commit Log & Index**: Segment layout, O(log N) binary index lookup, and startup recovery.
- **3. Fetch Flow**: Non-destructive reads, index seeking, CRC validation, and client offset advancement.
- **4. Offset Commit Flow**: Consumer group coordination, atomic file replacement, and restart survival.

---

### 5. Documentation Hub
An integrated documentation viewer in the dashboard that displays:
- `README.md`
- `MANUAL_TESTING_GUIDE.md`
- `ARCHITECTURE_AND_PIPELINE.md`

Includes a `COPY RAW MARKDOWN` utility for exporting documentation directly.

---

## Programmatic Java Client Usage

### Producer Example
```java
import client.Producer;
import client.Producer.RecordMetadata;

try (Producer producer = new Producer("127.0.0.1", 9092)) {
    RecordMetadata meta = producer.send("orders", "user_101", "{\"orderId\":\"ORD-99\",\"amount\":120.50}");
    System.out.println("Appended at offset: " + meta.offset + " in partition: " + meta.partition);
}
```

### Consumer Example
```java
import client.Consumer;
import model.Message;
import java.util.List;

try (Consumer consumer = new Consumer("127.0.0.1", 9092)) {
    consumer.subscribe("orders", 0);
    consumer.loadAndSeekCommittedOffset("analytics-workers");

    List<Message> messages = consumer.poll(10, 3000);
    for (Message m : messages) {
        System.out.println("Offset: " + m.getOffset() + " Key: " + m.getKeyAsString() + " Val: " + m.getValueAsString());
    }

    consumer.commitSync("analytics-workers");
}
```

---

## Technical Documentation
For detailed storage engine mechanics, wire protocol specifications, and sequence diagrams, refer to [ARCHITECTURE_AND_PIPELINE.md](./docs/ARCHITECTURE_AND_PIPELINE.md).
