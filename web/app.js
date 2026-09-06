// ==========================================================
// KAFKA CLONE NEO-BRUTALIST DASHBOARD JAVASCRIPT
// Handles API interactions, UI state, Disk Log Inspector,
// and the Interactive Architecture Pipeline Explainer.
// ==========================================================

const API_BASE = window.location.origin;

// State
let appState = {
    topics: [],
    selectedTopic: 'orders',
    selectedPartition: 0,
    consumerOffset: 0,
    polledRecords: [],
    autoPollInterval: null,
    currentFlow: 'produce',
    currentStepIndex: 0
};

// Architecture & Pipeline Step Definitions
const PIPELINES = {
    produce: {
        title: "PRODUCE FLOW: End-to-End Write Pipeline",
        steps: [
            {
                num: "STEP 1",
                title: "1. Producer Client Frame Encoding",
                desc: "The client creates a Message instance, computes a CRC32 checksum over the payload, determines the target partition (either explicitly or via key hash), packs the ProduceRequest, and prefixes it with a 4-byte big-endian frame length.",
                file: "src/client/Producer.java",
                code: `Message msg = Message.of(key, value);
ProduceRequest req = new ProduceRequest(
    corrId, topic, partition, (short) 1, 5000, Collections.singletonList(msg)
);
byte[] requestPayload = Protocol.encodeProduceRequest(req);
Protocol.writeFrame(out, requestPayload); // [4-byte length] + payload`
            },
            {
                num: "STEP 2",
                title: "2. Broker TCP Frame Read & Demux",
                desc: "Broker worker thread receives the TCP frame, verifies the 4-byte length guard (bounded to 32MB to prevent memory exhaustion), inspects the apiKey byte (1 = PRODUCE), and decodes the request parameters.",
                file: "src/broker/BrokerServer.java",
                code: `byte[] frame = Protocol.readFrame(in);
DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame));
byte apiKey = dis.readByte(); // 1 = PRODUCE
ProduceRequest req = Protocol.decodeProduceRequest(dis, correlationId);
CommitLog log = topicRegistry.getPartitionLog(req.topic, req.partition);`
            },
            {
                num: "STEP 3",
                title: "3. Partition Write Lock & Monotonic Offset",
                desc: "A ReentrantReadWriteLock writeLock is acquired for this partition. The next monotonic 64-bit offset is assigned via AtomicLong nextOffset.getAndIncrement(). This guarantees strict ordering across concurrent producer threads.",
                file: "src/storage/CommitLog.java",
                code: `rwLock.writeLock().lock();
try {
    long assignedOffset = nextOffset.getAndIncrement();
    Message msgWithOffset = original.withOffset(assignedOffset);
    activeSegment.append(msgWithOffset);
} finally {
    rwLock.writeLock().unlock();
}`
            },
            {
                num: "STEP 4",
                title: "4. Sequential FileChannel Append & Index Write",
                desc: "The current physical byte position in the .log file is captured and immediately recorded into the 16-byte .index entry [8B offset | 8B position]. The binary message is then sequentially appended to the active segment using Java NIO FileChannel.",
                file: "src/storage/CommitLog.java & OffsetIndex.java",
                code: `// 1. Record position in sparse/dense index
index.append(msg.getOffset(), channel.position());

// 2. Sequential disk write via NIO FileChannel
byte[] serialized = Protocol.serializeMessage(msg);
channel.write(ByteBuffer.wrap(serialized));

// 3. Roll segment if file size exceeds threshold (e.g., 1MB / 1GB)
if (activeSegment.getCurrentSize() >= maxSegmentBytes) {
    roll(nextOffset.get());
}`
            },
            {
                num: "STEP 5",
                title: "5. Acknowledgment (ACK) Frame Returned",
                desc: "The broker builds a ProduceResponse containing the assigned base offset, partition, and log append timestamp, wraps it in a length-prefixed TCP frame, and writes it back to the client socket.",
                file: "src/broker/BrokerServer.java",
                code: `ProduceResponse resp = new ProduceResponse(
    corrId, ErrorCode.NONE, req.topic, req.partition, baseOffset, System.currentTimeMillis()
);
byte[] responsePayload = Protocol.encodeProduceResponse(resp);
Protocol.writeFrame(out, responsePayload);`
            }
        ]
    },
    storage: {
        title: "STORAGE ENGINE: Append-Only Log & Binary Index",
        steps: [
            {
                num: "SEGMENT 1",
                title: "1. The Anatomy of a Log Segment (.log)",
                desc: "Partition data is broken into segment files named after their base offset (e.g. 00000000000000000000.log). Each message contains an 8-byte offset, 8-byte timestamp, 4-byte key length, key bytes, 4-byte value length, value bytes, and a 4-byte CRC32.",
                file: "src/model/Message.java",
                code: `// Storage Wire Layout (Binary):
// [Offset: 8B] [Timestamp: 8B]
// [KeyLen: 4B] [Key Bytes...]
// [ValLen: 4B] [Val Bytes...]
// [CRC32:  4B]`
            },
            {
                num: "SEGMENT 2",
                title: "2. The Offset Index (.index)",
                desc: "For every message appended, a fixed 16-byte entry is written to the paired .index file: [8 bytes offset][8 bytes byte-position in .log]. In-memory, a ConcurrentSkipListMap mirrors this structure for instant lookups.",
                file: "src/storage/OffsetIndex.java",
                code: `public synchronized void append(long offset, long position) throws IOException {
    inMemoryMap.put(offset, position);
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(offset);
    buf.putLong(position);
    buf.flip();
    channel.write(buf);
}`
            },
            {
                num: "SEGMENT 3",
                title: "3. O(log N) Floor Lookup",
                desc: "When a consumer wants offset 1,450, the index performs a floor lookup for the nearest offset <= 1,450. It returns the exact byte position to seek FileChannel to, completely avoiding expensive full file scans!",
                file: "src/storage/OffsetIndex.java",
                code: `public long lookup(long targetOffset) {
    Map.Entry<Long, Long> entry = inMemoryMap.floorEntry(targetOffset);
    return (entry != null) ? entry.getValue() : 0L;
}`
            },
            {
                num: "SEGMENT 4",
                title: "4. Crash Recovery on Broker Restart",
                desc: "When the broker boots, it inspects existing .log files in numerical order. It loads index entries and reads the active segment to restore nextOffset to the highest offset + 1. Zero data loss, zero manual recovery.",
                file: "src/storage/CommitLog.java",
                code: `File[] logFiles = partitionDir.listFiles((dir, name) -> name.endsWith(".log"));
Arrays.sort(logFiles, Comparator.comparing(File::getName));
for (File file : logFiles) {
    long base = Long.parseLong(file.getName().replace(".log", ""));
    Segment seg = new Segment(partitionDir, base);
    segments.put(base, seg);
}
nextOffset.set(highestOffset + 1);`
            }
        ]
    },
    fetch: {
        title: "FETCH FLOW: Sequential Consumer Polling",
        steps: [
            {
                num: "POLL 1",
                title: "1. Consumer Issues FetchRequest",
                desc: "Consumer specifies topic, partition, current offset pointer, and buffer limits (maxMessages, maxBytes). The request is transmitted over persistent TCP.",
                file: "src/client/Consumer.java",
                code: `FetchRequest req = new FetchRequest(
    corrId, topic, partition, currentOffset, maxMessages, maxBytes
);
Protocol.writeFrame(out, Protocol.encodeFetchRequest(req));`
            },
            {
                num: "POLL 2",
                title: "2. ReadLock & Index Direct Seek",
                desc: "The broker acquires a readLock (allowing multiple parallel consumers to read concurrently). It queries the OffsetIndex to find the floor file position for fetchOffset and positions the FileChannel directly.",
                file: "src/storage/CommitLog.java",
                code: `rwLock.readLock().lock();
try {
    long startPos = index.lookup(fromOffset);
    channel.position(startPos);
    channel.read(buf);
} finally {
    rwLock.readLock().unlock();
}`
            },
            {
                num: "POLL 3",
                title: "3. Non-Destructive Buffer Read & CRC Check",
                desc: "Messages are deserialized into memory without removing them from disk! The broker recalculates the CRC32 checksum to ensure zero silent bit-rot or file corruption before streaming back to the client.",
                file: "src/model/Message.java",
                code: `if (!msg.isChecksumValid()) {
    throw new CorruptMessageException("CRC mismatch at offset " + msg.getOffset());
}`
            },
            {
                num: "POLL 4",
                title: "4. Client Advances Monotonic Offset",
                desc: "Consumer client receives the batch. Upon receiving messages up to offset K, it updates its local pointer: currentOffset = K + 1, ready for the next poll cycle.",
                file: "src/client/Consumer.java",
                code: `if (!messages.isEmpty()) {
    currentOffset = messages.get(messages.size() - 1).getOffset() + 1;
}`
            }
        ]
    },
    commit: {
        title: "OFFSET COMMIT FLOW: Consumer Group Persistence",
        steps: [
            {
                num: "COMMIT 1",
                title: "1. Consumer Group Commit Request",
                desc: "After processing a batch of events, the consumer issues an OFFSET_COMMIT request with groupId, topic, partition, and processed offset.",
                file: "src/client/Consumer.java",
                code: `consumer.commitSync("analytics-workers");
// Sends: groupId:analytics-workers, topic:orders, partition:0, offset:42`
            },
            {
                num: "COMMIT 2",
                title: "2. Coordinator Map Update & Disk Flush",
                desc: "The broker stores the mapping in a ConcurrentHashMap and flushes to __consumer_offsets.dat using atomic temp-file replacement.",
                file: "src/broker/TopicRegistry.java",
                code: `committedOffsets.put(group + ":" + topic + ":" + partition, offset);
// Atomic write: write to temp file, then rename
tempFile.renameTo(offsetsFile);`
            },
            {
                num: "COMMIT 3",
                title: "3. Crash Recovery of Consumer Position",
                desc: "If consumer or broker restarts, Consumer.loadAndSeekCommittedOffset() queries the broker to resume reading exactly where the group left off without processing duplicate messages!",
                file: "src/client/Consumer.java",
                code: `long committed = consumer.fetchCommittedOffset("analytics-workers");
consumer.seek(committed); // Resume seamlessly!`
            }
        ]
    }
};

// ==========================================
// Initialization
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
    initTabs();
    initEventListeners();
    refreshBrokerStatus();
    loadTopics();
    renderPipeline();

    // Periodic status refresh
    setInterval(refreshBrokerStatus, 3000);
});

// ==========================================
// Tab Switching
// ==========================================

function initTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    tabButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            tabButtons.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            const tabId = btn.getAttribute('data-tab');
            document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
            const targetPane = document.getElementById(`tab-${tabId}`);
            if (targetPane) targetPane.classList.add('active');

            if (tabId === 'logviewer') {
                loadLogInspector();
            } else if (tabId === 'docs') {
                loadDoc(appState.currentDoc || 'readme');
            }
        });
    });

    // Pipeline flow toggles
    const flowToggles = document.querySelectorAll('.pill-toggle');
    flowToggles.forEach(toggle => {
        toggle.addEventListener('click', () => {
            flowToggles.forEach(t => t.classList.remove('active'));
            toggle.classList.add('active');
            appState.currentFlow = toggle.getAttribute('data-flow');
            appState.currentStepIndex = 0;
            renderPipeline();
        });
    });
}

// ==========================================
// Event Listeners
// ==========================================

function initEventListeners() {
    // Refresh all
    document.getElementById('refreshAllBtn').addEventListener('click', () => {
        refreshBrokerStatus();
        loadTopics();
    });

    // Produce form
    document.getElementById('produceForm').addEventListener('submit', handleProduceSubmit);

    // Payload template chips
    document.querySelectorAll('.chip').forEach(chip => {
        chip.addEventListener('click', () => {
            const tpl = chip.getAttribute('data-tpl');
            const valInput = document.getElementById('prodValueInput');
            const keyInput = document.getElementById('prodKeyInput');
            const randId = Math.floor(Math.random() * 9000 + 1000);

            if (tpl === 'order') {
                keyInput.value = `ord_${randId}`;
                valInput.value = JSON.stringify({
                    orderId: `ORD-${randId}`,
                    customerId: `CUST-${Math.floor(Math.random() * 500)}`,
                    amount: parseFloat((Math.random() * 200 + 10).toFixed(2)),
                    currency: "USD",
                    status: "CREATED",
                    timestamp: new Date().toISOString()
                }, null, 2);
            } else if (tpl === 'user') {
                keyInput.value = `user_${randId}`;
                valInput.value = JSON.stringify({
                    userId: `USR-${randId}`,
                    action: "USER_LOGIN",
                    ipAddress: `192.168.1.${randId % 255}`,
                    device: "Chrome/MacOS",
                    timestamp: new Date().toISOString()
                }, null, 2);
            } else if (tpl === 'payment') {
                keyInput.value = `pay_${randId}`;
                valInput.value = JSON.stringify({
                    paymentId: `PAY-${randId}`,
                    gateway: "STRIPE",
                    status: "SETTLED",
                    authCode: "AUTH_" + Math.random().toString(36).substring(7).toUpperCase()
                }, null, 2);
            }
        });
    });

    // Consumer controls
    document.getElementById('pollMessagesBtn').addEventListener('click', () => pollMessages(10));
    document.getElementById('consSeekBtn').addEventListener('click', handleSeekOffset);
    document.getElementById('commitOffsetBtn').addEventListener('click', handleCommitOffset);
    document.getElementById('clearPollBtn').addEventListener('click', () => {
        appState.polledRecords = [];
        renderConsumedFeed();
    });

    // Auto-poll toggle
    document.getElementById('toggleAutoPollBtn').addEventListener('click', () => {
        const btn = document.getElementById('toggleAutoPollBtn');
        if (appState.autoPollInterval) {
            clearInterval(appState.autoPollInterval);
            appState.autoPollInterval = null;
            btn.textContent = "AUTO: OFF";
            btn.classList.remove('btn-lime');
            btn.classList.add('btn-white');
        } else {
            appState.autoPollInterval = setInterval(() => pollMessages(5), 1200);
            btn.textContent = "AUTO: ON (1.2s)";
            btn.classList.remove('btn-white');
            btn.classList.add('btn-lime');
        }
    });

    // Target change updates
    document.getElementById('prodTopicSelect').addEventListener('change', (e) => {
        appState.selectedTopic = e.target.value;
        updateTargetBadge();
    });
    document.getElementById('prodPartitionSelect').addEventListener('change', (e) => {
        appState.selectedPartition = parseInt(e.target.value);
        updateTargetBadge();
    });

    // Create topic modal
    document.getElementById('openCreateTopicModalBtn').addEventListener('click', () => {
        document.getElementById('createTopicModal').classList.remove('hidden');
    });
    document.getElementById('closeModalBtn').addEventListener('click', () => {
        document.getElementById('createTopicModal').classList.add('hidden');
    });
    document.getElementById('cancelModalBtn').addEventListener('click', () => {
        document.getElementById('createTopicModal').classList.add('hidden');
    });
    document.getElementById('createTopicForm').addEventListener('submit', handleCreateTopic);

    // Inspector controls
    document.getElementById('inspectRefreshBtn').addEventListener('click', loadLogInspector);
    document.getElementById('inspectTopicSelect').addEventListener('change', loadLogInspector);
    document.getElementById('inspectPartSelect').addEventListener('change', loadLogInspector);

    // Documentation Hub listeners
    document.querySelectorAll('.doc-tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const docName = btn.getAttribute('data-doc');
            switchDoc(docName);
        });
    });

    const copyBtn = document.getElementById('copyDocBtn');
    if (copyBtn) copyBtn.addEventListener('click', copyActiveDoc);
}

function updateTargetBadge() {
    document.getElementById('producerTargetBadge').textContent = `TARGET: ${appState.selectedTopic}:${appState.selectedPartition}`;
}

// ==========================================
// API Handlers
// ==========================================

async function refreshBrokerStatus() {
    try {
        const res = await fetch(`${API_BASE}/api/status`);
        if (!res.ok) throw new Error("Broker offline");
        const data = await res.json();

        document.getElementById('statTcpPort').textContent = data.tcpPort || 9092;
        document.getElementById('statHttpPort').textContent = data.httpPort || 8080;
        document.getElementById('statTotalMsgs').textContent = (data.totalMessages || 0).toLocaleString();
        document.getElementById('statUptime').textContent = `${data.uptimeSeconds || 0}s`;

        document.getElementById('clusterStatusText').textContent = "BROKER ONLINE";
        document.querySelector('.status-dot').className = "status-dot pulsing";
    } catch (e) {
        document.getElementById('clusterStatusText').textContent = "DISCONNECTED";
        document.querySelector('.status-dot').className = "status-dot";
    }
}

async function loadTopics() {
    try {
        const res = await fetch(`${API_BASE}/api/topics`);
        const topics = await res.json();
        appState.topics = topics;

        // Populate selects
        populateTopicSelects(topics);

        // Render Topics grid
        renderTopicsGrid(topics);
    } catch (e) {
        console.error("Failed to load topics:", e);
    }
}

function populateTopicSelects(topics) {
    const selects = ['prodTopicSelect', 'consTopicSelect', 'inspectTopicSelect'];
    selects.forEach(selId => {
        const sel = document.getElementById(selId);
        const currVal = sel.value;
        sel.innerHTML = '';
        topics.forEach(t => {
            const opt = document.createElement('option');
            opt.value = t.topic;
            opt.textContent = t.topic;
            if (t.topic === currVal || (t.topic === appState.selectedTopic && !currVal)) {
                opt.selected = true;
            }
            sel.appendChild(opt);
        });
        if (sel.options.length > 0 && !sel.value) {
            sel.selectedIndex = 0;
        }
    });

    if (topics.length > 0 && !appState.selectedTopic) {
        appState.selectedTopic = topics[0].topic;
    }
    updateTargetBadge();
}

function renderTopicsGrid(topics) {
    const container = document.getElementById('topicsContainer');
    if (!topics || topics.length === 0) {
        container.innerHTML = `<div class="empty-state">No topics found. Create your first topic!</div>`;
        return;
    }

    container.innerHTML = '';
    topics.forEach(t => {
        const card = document.createElement('div');
        card.className = `topic-card ${t.topic === appState.selectedTopic ? 'active' : ''}`;
        card.addEventListener('click', () => {
            appState.selectedTopic = t.topic;
            document.querySelectorAll('.topic-card').forEach(c => c.classList.remove('active'));
            card.classList.add('active');
            document.getElementById('prodTopicSelect').value = t.topic;
            document.getElementById('consTopicSelect').value = t.topic;
            document.getElementById('inspectTopicSelect').value = t.topic;
            updateTargetBadge();
        });

        let partitionsHtml = '';
        t.partitions.forEach(p => {
            partitionsHtml += `
                <div class="partition-badge-row">
                    <span>Partition ${p.partition}</span>
                    <span>High Watermark: <strong>${p.highWatermark}</strong></span>
                </div>
            `;
        });

        card.innerHTML = `
            <div class="topic-card-header">
                <span class="topic-card-name"># ${escapeHtml(t.topic)}</span>
                <span class="pill-badge pill-black">${t.partitions.length} PARTITIONS</span>
            </div>
            <div class="partitions-list">
                ${partitionsHtml}
            </div>
        `;
        container.appendChild(card);
    });
}

// ==========================================
// Produce Message
// ==========================================

async function handleProduceSubmit(e) {
    e.preventDefault();
    const topic = document.getElementById('prodTopicSelect').value;
    const partition = parseInt(document.getElementById('prodPartitionSelect').value);
    const key = document.getElementById('prodKeyInput').value.trim();
    const value = document.getElementById('prodValueInput').value.trim();

    if (!topic) {
        alert("Please select or create a topic first.");
        return;
    }

    const submitBtn = document.getElementById('produceSubmitBtn');
    submitBtn.disabled = true;
    submitBtn.textContent = "WRITING TO COMMIT LOG...";

    const t0 = performance.now();
    try {
        const res = await fetch(`${API_BASE}/api/produce`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ topic, partition, key, value })
        });
        const data = await res.json();
        const latency = (performance.now() - t0).toFixed(1);

        if (data.success) {
            // Show receipt
            const receipt = document.getElementById('producerReceipt');
            receipt.classList.remove('hidden');
            document.getElementById('rcptOffset').textContent = `#${data.offset}`;
            document.getElementById('rcptPart').textContent = `${data.topic}:${data.partition}`;
            document.getElementById('rcptCrc').textContent = data.crc;
            document.getElementById('rcptTs').textContent = `${new Date(data.timestamp).toLocaleTimeString()} (${latency}ms)`;

            // Flash receipt
            receipt.style.transform = 'scale(1.02)';
            setTimeout(() => { receipt.style.transform = 'scale(1)'; }, 150);

            // Refresh topics
            loadTopics();
            refreshBrokerStatus();
        } else {
            alert("Produce failed: " + (data.error || "Unknown error"));
        }
    } catch (err) {
        alert("Network error communicating with broker: " + err.message);
    } finally {
        submitBtn.disabled = false;
        submitBtn.textContent = "SEND TO BROKER (O(1) DISK APPEND)";
    }
}

// ==========================================
// Consumer Polling & Committing
// ==========================================

async function pollMessages(limit = 10) {
    const topic = document.getElementById('consTopicSelect').value;
    const partition = parseInt(document.getElementById('consPartSelect').value);
    const offset = appState.consumerOffset;

    if (!topic) return;

    try {
        const res = await fetch(`${API_BASE}/api/consume?topic=${encodeURIComponent(topic)}&partition=${partition}&offset=${offset}&limit=${limit}`);
        const data = await res.json();

        if (data.messages && data.messages.length > 0) {
            // Append messages to polled records
            data.messages.forEach(m => {
                appState.polledRecords.unshift(m); // new messages on top
            });

            // Advance consumer offset pointer
            appState.consumerOffset = data.nextOffset;
            document.getElementById('consumerPosBadge').textContent = `POS: ${appState.consumerOffset}`;
            document.getElementById('consSeekInput').value = appState.consumerOffset;

            renderConsumedFeed();
        }
    } catch (e) {
        console.error("Poll error:", e);
    }
}

function handleSeekOffset() {
    const seekVal = parseInt(document.getElementById('consSeekInput').value);
    if (!isNaN(seekVal) && seekVal >= 0) {
        appState.consumerOffset = seekVal;
        document.getElementById('consumerPosBadge').textContent = `POS: ${seekVal}`;
        pollMessages(10);
    }
}

async function handleCommitOffset() {
    const groupId = document.getElementById('consGroupInput').value.trim();
    const topic = document.getElementById('consTopicSelect').value;
    const partition = parseInt(document.getElementById('consPartSelect').value);
    const offset = appState.consumerOffset;

    if (!groupId) {
        alert("Enter a Consumer Group ID");
        return;
    }

    try {
        const res = await fetch(`${API_BASE}/api/commit`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ groupId, topic, partition, offset })
        });
        const data = await res.json();
        if (data.success) {
            const btn = document.getElementById('commitOffsetBtn');
            const originalText = btn.textContent;
            btn.textContent = `COMMITTED (OFFSET ${offset})`;
            btn.style.background = '#00E676';
            setTimeout(() => {
                btn.textContent = originalText;
                btn.style.background = '';
            }, 1800);
        }
    } catch (e) {
        alert("Offset commit failed: " + e.message);
    }
}

function renderConsumedFeed() {
    const container = document.getElementById('consumedRecordsList');
    document.getElementById('pollCount').textContent = appState.polledRecords.length;

    if (appState.polledRecords.length === 0) {
        container.innerHTML = `<div class="empty-state">No records polled yet. Click [POLL (NEXT 10)] above.</div>`;
        return;
    }

    container.innerHTML = '';
    appState.polledRecords.slice(0, 30).forEach(m => {
        const card = document.createElement('div');
        card.className = 'record-card';
        card.innerHTML = `
            <div class="record-top">
                <span class="record-offset">OFFSET #${m.offset}</span>
                <span class="pill-badge pill-green">CRC OK: ${m.crc}</span>
                <span class="dim mono">${new Date(m.timestamp).toLocaleTimeString()}</span>
            </div>
            ${m.key ? `<div class="record-key">KEY: <strong>${escapeHtml(m.key)}</strong></div>` : ''}
            <div class="record-val">${escapeHtml(m.value)}</div>
        `;
        container.appendChild(card);
    });
}

// ==========================================
// Log & Index Inspector
// ==========================================

async function loadLogInspector() {
    const topic = document.getElementById('inspectTopicSelect').value || appState.selectedTopic;
    const partition = parseInt(document.getElementById('inspectPartSelect').value || 0);

    if (!topic) return;

    try {
        const res = await fetch(`${API_BASE}/api/log-viewer?topic=${encodeURIComponent(topic)}&partition=${partition}`);
        const data = await res.json();

        document.getElementById('inspectDir').textContent = data.partitionDir || '-';
        document.getElementById('inspectHwm').textContent = `#${data.highWatermark || 0}`;
        document.getElementById('inspectSegCount').textContent = `${(data.segments || []).length} Segment(s)`;

        // Render segments table
        const tbody = document.getElementById('segmentsTableBody');
        if (!data.segments || data.segments.length === 0) {
            tbody.innerHTML = `<tr><td colspan="6" class="text-center">No segments found on disk.</td></tr>`;
        } else {
            tbody.innerHTML = '';
            data.segments.forEach((seg, idx) => {
                const tr = document.createElement('tr');
                const isLast = (idx === data.segments.length - 1);
                tr.innerHTML = `
                    <td><strong>${seg.baseOffset}</strong></td>
                    <td class="mono">${escapeHtml(seg.logFile)}</td>
                    <td class="mono">${escapeHtml(seg.logFile.replace('.log', '.index'))}</td>
                    <td><strong>${seg.logSizeBytes.toLocaleString()}</strong> Bytes</td>
                    <td><span class="pill-badge pill-yellow">${seg.indexEntriesCount} ENTRIES</span></td>
                    <td>${isLast ? '<span class="pill-badge pill-lime">ACTIVE APPENDING</span>' : '<span class="pill-badge pill-black">ROLLED (READ-ONLY)</span>'}</td>
                `;
                tbody.appendChild(tr);
            });
        }

        // Render disk records grid
        const recordsGrid = document.getElementById('diskRecordsList');
        if (!data.messages || data.messages.length === 0) {
            recordsGrid.innerHTML = `<div class="empty-state">No messages stored in this partition yet.</div>`;
        } else {
            recordsGrid.innerHTML = '';
            data.messages.forEach(m => {
                const item = document.createElement('div');
                item.className = 'disk-record-item';
                item.innerHTML = `
                    <div class="record-top">
                        <span class="record-offset">OFFSET #${m.offset}</span>
                        <span class="mono" style="font-size:0.75rem;">CRC: ${m.crc}</span>
                    </div>
                    <div class="mono" style="font-size:0.75rem;"><strong>KEY:</strong> ${m.key ? escapeHtml(m.key) : '<em>null</em>'}</div>
                    <div class="record-val">${escapeHtml(m.value)}</div>
                `;
                recordsGrid.appendChild(item);
            });
        }
    } catch (e) {
        console.error("Log inspector error:", e);
    }
}

// ==========================================
// Create Topic Modal
// ==========================================

async function handleCreateTopic(e) {
    e.preventDefault();
    const topic = document.getElementById('newTopicName').value.trim();
    const partitions = parseInt(document.getElementById('newTopicPartitions').value);

    if (!topic) return;

    try {
        const res = await fetch(`${API_BASE}/api/create-topic`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ topic, partitions })
        });
        const data = await res.json();
        if (data.success) {
            document.getElementById('createTopicModal').classList.add('hidden');
            document.getElementById('newTopicName').value = '';
            appState.selectedTopic = topic;
            loadTopics();
        } else {
            alert("Topic creation failed: " + (data.error || "Unknown error"));
        }
    } catch (err) {
        alert("Error creating topic: " + err.message);
    }
}

// ==========================================
// Pipeline & Architecture Visualizer
// ==========================================

function renderPipeline() {
    const flow = PIPELINES[appState.currentFlow];
    if (!flow) return;

    const container = document.getElementById('pipelineDiagram');
    container.innerHTML = '';

    flow.steps.forEach((step, idx) => {
        const node = document.createElement('div');
        node.className = `pipeline-node ${idx === appState.currentStepIndex ? 'selected' : ''}`;
        node.addEventListener('click', () => {
            appState.currentStepIndex = idx;
            document.querySelectorAll('.pipeline-node').forEach(n => n.classList.remove('selected'));
            node.classList.add('selected');
            updatePipelineDetail(step);
        });

        node.innerHTML = `
            <span class="node-num">${step.num}</span>
            <div class="node-title">${escapeHtml(step.title.replace(/^[0-9]+\.\s*/, ''))}</div>
            <div class="node-sub mono">${escapeHtml(step.file.split('/').pop())}</div>
        `;
        container.appendChild(node);

        if (idx < flow.steps.length - 1) {
            const arrow = document.createElement('div');
            arrow.className = 'pipeline-arrow';
            arrow.textContent = '➔';
            container.appendChild(arrow);
        }
    });

    updatePipelineDetail(flow.steps[appState.currentStepIndex]);
}

function updatePipelineDetail(step) {
    document.getElementById('detailStepNum').textContent = step.num;
    document.getElementById('detailStepTitle').textContent = step.title;
    document.getElementById('detailStepDesc').textContent = step.desc;
    document.getElementById('detailCodeFile').textContent = step.file;
    document.getElementById('detailCodeContent').textContent = step.code;
}

// Helper: Escape HTML
function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

// ==========================================
// Documentation Hub Handlers
// ==========================================

let rawDocCache = {};
let activeDocName = 'readme';

window.switchDoc = function(docName) {
    activeDocName = docName;
    appState.currentDoc = docName;

    // Switch active tab button styling
    document.querySelectorAll('.doc-tab-btn').forEach(btn => {
        if (btn.getAttribute('data-doc') === docName) {
            btn.className = 'btn btn-sm btn-yellow active doc-tab-btn';
        } else {
            btn.className = 'btn btn-sm btn-white doc-tab-btn';
        }
    });

    // Make sure docs tab is visible
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelector('[data-tab="docs"]').classList.add('active');
    document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
    document.getElementById('tab-docs').classList.add('active');

    loadDoc(docName);
};

async function loadDoc(docName) {
    activeDocName = docName;
    const badge = document.getElementById('activeDocBadge');
    const pathLabel = document.getElementById('activeDocPath');
    const container = document.getElementById('docContentRendered');

    let fileName = 'README.md';
    let repoPath = 'kafka-clone/README.md';
    if (docName === 'testing') {
        fileName = 'MANUAL_TESTING_GUIDE.md';
        repoPath = 'kafka-clone/MANUAL_TESTING_GUIDE.md';
    } else if (docName === 'architecture') {
        fileName = 'ARCHITECTURE_AND_PIPELINE.md';
        repoPath = 'kafka-clone/docs/ARCHITECTURE_AND_PIPELINE.md';
    }

    badge.textContent = fileName;
    pathLabel.textContent = repoPath;

    if (rawDocCache[docName]) {
        container.innerHTML = renderSimpleMarkdown(rawDocCache[docName]);
        return;
    }

    container.innerHTML = '<div class="empty-state">Loading document from broker...</div>';

    try {
        const res = await fetch(`${API_BASE}/api/docs?name=${encodeURIComponent(docName)}`);
        const data = await res.json();
        rawDocCache[docName] = data.content || '';
        container.innerHTML = renderSimpleMarkdown(data.content || '');
    } catch (e) {
        container.innerHTML = `<div class="empty-state" style="color:red;">Failed to load document: ${e.message}</div>`;
    }
}

function copyActiveDoc() {
    const raw = rawDocCache[activeDocName] || '';
    if (!raw) return;
    navigator.clipboard.writeText(raw).then(() => {
        const btn = document.getElementById('copyDocBtn');
        const orig = btn.textContent;
        btn.textContent = 'COPIED TO CLIPBOARD';
        btn.style.background = '#70FF00';
        setTimeout(() => {
            btn.textContent = orig;
            btn.style.background = '';
        }, 2000);
    });
}

function renderSimpleMarkdown(md) {
    if (!md) return '<p>No content</p>';

    // Escape basic HTML
    let out = md
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");

    // Code blocks
    out = out.replace(/```([a-z0-9_-]*)\n([\s\S]*?)```/g, (match, lang, code) => {
        return `<pre><code>${code}</code></pre>`;
    });

    // Inline code
    out = out.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Headers
    out = out.replace(/^### (.*$)/gim, '<h3>$1</h3>');
    out = out.replace(/^## (.*$)/gim, '<h2>$1</h2>');
    out = out.replace(/^# (.*$)/gim, '<h1>$1</h1>');

    // Blockquotes
    out = out.replace(/^\> (.*$)/gim, '<blockquote>$1</blockquote>');

    // Bold & italic
    out = out.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    out = out.replace(/\*([^*]+)\*/g, '<em>$1</em>');

    // Unordered lists
    out = out.replace(/^\s*-\s+(.*$)/gim, '<li>$1</li>');
    out = out.replace(/(<li>.*<\/li>)/gms, '<ul>$1</ul>');

    // Tables
    out = out.replace(/^\|(.+)\|$/gim, (match, row) => {
        const cells = row.split('|').map(c => c.trim());
        if (cells.every(c => /^:?-+:?$/.test(c))) {
            return ''; // separator row
        }
        const cellTags = cells.map(c => `<td>${c}</td>`).join('');
        return `<tr>${cellTags}</tr>`;
    });
    out = out.replace(/((?:<tr>.*<\/tr>\s*)+)/g, '<table>$1</table>');

    // Paragraphs
    const lines = out.split('\n\n');
    out = lines.map(block => {
        block = block.trim();
        if (!block) return '';
        if (/^<(h[1-6]|ul|ol|pre|table|blockquote)/i.test(block)) {
            return block;
        }
        return `<p>${block.replace(/\n/g, '<br>')}</p>`;
    }).join('\n');

    return out;
}
