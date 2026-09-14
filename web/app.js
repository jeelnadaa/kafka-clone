// ==========================================================
// KAFKA CLONE NEO-BRUTALIST DASHBOARD JAVASCRIPT
// Handles API interactions, UI state, Disk Log Inspector,
// and the Interactive Architecture Pipeline Explainer.
// ==========================================================

const API_BASE = window.location.origin;

// State
let appState = {
    topics: [],
    selectedTopic: '',
    selectedPartition: 'auto',
    activeGroupId: 'analytics-workers',
    clientRoundRobinSeq: 0,
    consumerGroups: {
        'analytics-workers': {
            strategy: 'auto-range',
            consumers: [
                {
                    id: 'c-1',
                    name: 'Consumer #1',
                    manualPartitions: [],
                    assignedPartitions: [],
                    offsets: {},
                    polledRecords: [],
                    autoPollInterval: null
                }
            ]
        },
        'billing-service': {
            strategy: 'auto-range',
            consumers: [
                {
                    id: 'c-1',
                    name: 'Consumer #1',
                    manualPartitions: [],
                    assignedPartitions: [],
                    offsets: {},
                    polledRecords: [],
                    autoPollInterval: null
                }
            ]
        },
        'notification-fleet': {
            strategy: 'auto-range',
            consumers: [
                {
                    id: 'c-1',
                    name: 'Consumer #1',
                    manualPartitions: [],
                    assignedPartitions: [],
                    offsets: {},
                    polledRecords: [],
                    autoPollInterval: null
                }
            ]
        }
    },
    groupAutoPollInterval: null,
    currentFlow: 'producer',
    currentStepIndex: 0
};

// ==========================================
// Architecture & Pipeline Step Definitions
// 3 Core Roles: Producer, Broker, Consumer
// ==========================================
const PIPELINES = {
    producer: {
        title: "PRODUCER: How Messages Are Produced",
        steps: [
            {
                num: "STEP 1",
                title: "1. Pick Partition & Create Message Object",
                desc: "The Producer chooses the target partition (either by hashing the key like user_123 so the same key always lands in the same partition, or using round-robin). It then creates a Message object holding the key, value, timestamp, and a calculated CRC32 checksum for data integrity.",
                file: "src/client/Producer.java",
                code: `// 1. Determine target partition (Key Hash or Round-Robin)
int partition = (key != null) ? Math.abs(key.hashCode() % pCount) : roundRobinSeq++ % pCount;

// 2. Create Message object (computes CRC32 checksum)
Message msg = Message.of(key, value);`
            },
            {
                num: "STEP 2",
                title: "2. Create ProduceRequest & Convert to Binary",
                desc: "The Producer wraps the Message into a ProduceRequest object containing the topic and partition. It then converts this ProduceRequest object into binary bytes using Protocol.encodeProduceRequest(), prefixes it with a 4-byte frame length header, and sends it directly over the TCP socket to the broker.",
                file: "src/client/Producer.java",
                code: `// 1. Wrap in ProduceRequest object
ProduceRequest req = new ProduceRequest(topic, partition, Collections.singletonList(msg));

// 2. Convert to binary bytes and send over TCP socket
byte[] requestPayload = Protocol.encodeProduceRequest(req);
Protocol.writeFrame(socketOut, requestPayload); // [4-byte length] + [binary payload]`
            },
            {
                num: "STEP 3",
                title: "3. Receive Broker Acknowledgment (ACK)",
                desc: "The Producer waits on the TCP socket for the broker's response. The broker replies with a binary frame which the client decodes into a ProduceResponse object containing the assigned sequential offset (e.g. #42), confirming that the message is durably written to disk.",
                file: "src/client/Producer.java",
                code: `// Read binary response from broker and decode
byte[] responseFrame = Protocol.readFrame(socketIn);
ProduceResponse resp = Protocol.decodeProduceResponse(
    new DataInputStream(new ByteArrayInputStream(responseFrame))
);
System.out.println("Saved to " + resp.topic + ":" + resp.partition + " at offset #" + resp.baseOffset);`
            }
        ]
    },
    broker: {
        title: "BROKER: How Messages Are Saved to Disk",
        steps: [
            {
                num: "STEP 1",
                title: "1. Decode ProduceRequest & Lock Partition",
                desc: "The broker receives the binary TCP frame, decodes it into a ProduceRequest object, and retrieves the CommitLog for that topic and partition. It acquires a partition write lock (ReentrantReadWriteLock) and assigns the next sequential offset (e.g. #42) using an atomic counter so messages are strictly ordered.",
                file: "src/storage/CommitLog.java",
                code: `// 1. Decode binary request into ProduceRequest object
ProduceRequest req = Protocol.decodeProduceRequest(dis, correlationId);
CommitLog log = topicRegistry.getPartitionLog(req.topic, req.partition);

// 2. Lock partition and assign next sequential offset
rwLock.writeLock().lock();
try {
    long assignedOffset = nextOffset.getAndIncrement(); // e.g. #42
    Message stampedMsg = originalMsg.withOffset(assignedOffset);
    activeSegment.append(stampedMsg);
} finally {
    rwLock.writeLock().unlock();
}`
            },
            {
                num: "STEP 2",
                title: "2. Append Binary Bytes to .log File (O(1) Speed)",
                desc: "The broker converts the message into binary bytes and appends it directly to the active .log segment file using Java NIO FileChannel. Because it strictly appends to the end of the file and never modifies existing data, this sequential disk write is extremely fast (O(1)) without random disk seek delays.",
                file: "src/storage/CommitLog.java",
                code: `// Convert message to binary bytes and append to .log file
byte[] serialized = Protocol.serializeMessage(stampedMsg);
logChannel.write(ByteBuffer.wrap(serialized)); // O(1) Sequential Disk Append`
            },
            {
                num: "STEP 3",
                title: "3. Record Offset in .index File (O(log N) Lookup)",
                desc: "Immediately after writing to the .log file, the broker records a 16-byte entry in the companion .index file: [8 bytes offset | 8 bytes file byte position]. When consumers later want to read offset #42, the broker uses binary search on this .index file to jump straight to byte position 4,096 instead of scanning the whole file from start to finish.",
                file: "src/storage/OffsetIndex.java",
                code: `// Record 16-byte bookmark: [Offset: 8B | File Byte Position: 8B]
long physicalBytePos = logChannel.position();
index.append(assignedOffset, physicalBytePos); // Maps offset #42 -> byte 4,096`
            }
        ]
    },
    consumer: {
        title: "CONSUMER: How Messages Are Consumed",
        steps: [
            {
                num: "STEP 1",
                title: "1. Create FetchRequest with Current Offset",
                desc: "The Consumer creates a FetchRequest object specifying the topic, assigned partition, its current reading offset pointer (e.g. offset = 20), and max batch size (e.g. 10 messages). It converts this request object into binary bytes and sends it over the TCP socket to the broker.",
                file: "src/client/Consumer.java",
                code: `// 1. Create FetchRequest object with current offset pointer
FetchRequest req = new FetchRequest(corrId, topic, partition, currentOffset, 10, 1024 * 1024);

// 2. Convert to binary bytes and send over TCP
byte[] payload = Protocol.encodeFetchRequest(req);
Protocol.writeFrame(socketOut, payload);`
            },
            {
                num: "STEP 2",
                title: "2. Broker Reads .log File & Verifies Checksum",
                desc: "The broker receives the request, acquires a read lock (so multiple consumers can read simultaneously), and checks the .index file to jump FileChannel directly to that byte position. It reads the binary bytes from the .log file, verifies the CRC32 checksum to make sure data isn't corrupted, and returns a FetchResponse. Crucially, reading does NOT delete messages from disk.",
                file: "src/storage/CommitLog.java",
                code: `rwLock.readLock().lock();
try {
    // 1. Jump directly to byte position from .index file
    long startPos = index.lookup(fetchOffset);
    channel.position(startPos);
    channel.read(buffer); // Non-destructive read from disk
} finally {
    rwLock.readLock().unlock();
}`
            },
            {
                num: "STEP 3",
                title: "3. Parse Messages, Advance Offset & Commit",
                desc: "The Consumer receives the binary response, parses it into a list of Message objects, and processes them. It advances its local offset pointer from #20 to #30. Finally, it creates an OffsetCommitRequest and sends it to the broker, which saves the group's committed offset into __consumer_offsets.dat on disk so it can resume after restarts.",
                file: "src/client/Consumer.java",
                code: `// 1. Parse binary response into Message objects and advance pointer
List<Message> messages = response.getMessages();
this.currentOffset = response.getNextOffset(); // e.g. advances 20 -> 30

// 2. Commit progress to broker storage (__consumer_offsets.dat)
consumer.commitSync("analytics-workers");`
            }
        ]
    }
};

// ==========================================
// Initialization
// ==========================================

document.addEventListener('DOMContentLoaded', async () => {
    initTabs();
    initEventListeners();
    refreshBrokerStatus();
    await loadTopics();
    renderPipeline();
    renderGroupChips();

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
    document.getElementById('refreshAllBtn').addEventListener('click', async () => {
        refreshBrokerStatus();
        await loadTopics();
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
            } else if (tpl === 'text') {
                keyInput.value = `txt_${randId}`;
                valInput.value = `Order notification: Transaction TX-${randId} completed for customer Alice at ${new Date().toLocaleTimeString()}`;
            }
        });
    });

    // Multi-Consumer Cluster controls
    const addConsBtn = document.getElementById('addConsumerInstanceBtn');
    if (addConsBtn) addConsBtn.addEventListener('click', addConsumerInstance);

    const pollAllBtn = document.getElementById('pollAllConsumersBtn');
    if (pollAllBtn) pollAllBtn.addEventListener('click', pollAllConsumers);

    const autoAllBtn = document.getElementById('toggleAutoPollAllBtn');
    if (autoAllBtn) autoAllBtn.addEventListener('click', toggleAutoPollAll);

    // Assignment Strategy buttons
    const stratRange = document.getElementById('stratRangeBtn');
    if (stratRange) stratRange.addEventListener('click', () => setAssignmentStrategy('auto-range'));

    const stratRR = document.getElementById('stratRoundRobinBtn');
    if (stratRR) stratRR.addEventListener('click', () => setAssignmentStrategy('auto-rr'));

    const stratManual = document.getElementById('stratManualBtn');
    if (stratManual) stratManual.addEventListener('click', () => setAssignmentStrategy('manual'));

    // Consumer Group modal & creation listeners
    const openGroupModalBtn = document.getElementById('openCreateGroupModalBtn');
    if (openGroupModalBtn) openGroupModalBtn.addEventListener('click', openCreateGroupModal);


    const closeGroupModalBtn = document.getElementById('closeGroupModalBtn');
    if (closeGroupModalBtn) closeGroupModalBtn.addEventListener('click', closeCreateGroupModal);

    const cancelGroupModalBtn = document.getElementById('cancelGroupModalBtn');
    if (cancelGroupModalBtn) cancelGroupModalBtn.addEventListener('click', closeCreateGroupModal);

    const createGroupForm = document.getElementById('createGroupForm');
    if (createGroupForm) createGroupForm.addEventListener('submit', handleCreateGroup);

    // Target change updates
    document.getElementById('prodTopicSelect').addEventListener('change', async (e) => {
        appState.selectedTopic = e.target.value;
        const consSel = document.getElementById('consTopicSelect');
        if (consSel) consSel.value = e.target.value;
        const inspSel = document.getElementById('inspectTopicSelect');
        if (inspSel) inspSel.value = e.target.value;
        await syncCommittedOffsets(appState.activeGroupId, e.target.value);
        updatePartitionSelects(e.target.value);
        updateTargetBadge();
    });

    const consTopicSel = document.getElementById('consTopicSelect');
    if (consTopicSel) consTopicSel.addEventListener('change', async (e) => {
        appState.selectedTopic = e.target.value;
        document.getElementById('prodTopicSelect').value = e.target.value;
        document.getElementById('inspectTopicSelect').value = e.target.value;
        await syncCommittedOffsets(appState.activeGroupId, e.target.value);
        updatePartitionSelects(e.target.value);
        updateTargetBadge();
    });

    const consGroupInput = document.getElementById('consGroupInput');
    if (consGroupInput) {
        consGroupInput.addEventListener('change', (e) => {
            const val = e.target.value.trim();
            if (val) switchConsumerGroup(val);
        });
        consGroupInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                const val = e.target.value.trim();
                if (val) switchConsumerGroup(val);
            }
        });
    }

    document.getElementById('prodPartitionSelect').addEventListener('change', (e) => {
        appState.selectedPartition = e.target.value;
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

    // Seek warning modal controls
    const closeSeekBtn = document.getElementById('closeSeekModalBtn');
    if (closeSeekBtn) closeSeekBtn.addEventListener('click', closeSeekModal);
    const ackSeekBtn = document.getElementById('ackSeekModalBtn');
    if (ackSeekBtn) ackSeekBtn.addEventListener('click', closeSeekModal);
    const seekModal = document.getElementById('seekWarningModal');
    if (seekModal) {
        seekModal.addEventListener('click', (e) => {
            if (e.target === seekModal) closeSeekModal();
        });
    }
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeSeekModal();
    });

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

async function syncCommittedOffsets(groupId, topicName) {
    if (!groupId || !topicName) return;
    const group = getOrCreateGroup(groupId);
    try {
        const res = await fetch(`${API_BASE}/api/commit?groupId=${encodeURIComponent(groupId)}&topic=${encodeURIComponent(topicName)}`);
        if (!res.ok) return;
        const data = await res.json();
        const serverOffsets = data.offsets || {};
        group.consumers.forEach(c => {
            if (!c.offsets) c.offsets = {};
            for (const [pStr, off] of Object.entries(serverOffsets)) {
                const p = parseInt(pStr);
                if (off !== undefined && off >= 0) {
                    c.offsets[p] = off;
                }
            }
        });
    } catch (e) {
        console.error("Failed to sync committed offsets:", e);
    }
}

async function loadTopics() {
    try {
        const res = await fetch(`${API_BASE}/api/topics`);
        const topics = await res.json();
        appState.topics = topics;

        // Auto-select valid topic
        if (topics.length > 0) {
            const topicExists = topics.some(t => t.topic === appState.selectedTopic);
            if (!topicExists) {
                appState.selectedTopic = topics[0].topic;
            }
        }

        // Pre-fetch committed offsets from the broker before rendering consumers
        if (appState.selectedTopic) {
            await syncCommittedOffsets(appState.activeGroupId, appState.selectedTopic);
        }

        // Populate selects
        populateTopicSelects(topics);

        // Render Topics grid
        renderTopicsGrid(topics);
    } catch (e) {
        console.error("Failed to load topics:", e);
    }
}

function populateTopicSelects(topics) {
    if (topics.length > 0) {
        const topicExists = topics.some(t => t.topic === appState.selectedTopic);
        if (!topicExists) {
            appState.selectedTopic = topics[0].topic;
        }
    }

    const selects = ['prodTopicSelect', 'consTopicSelect', 'inspectTopicSelect'];
    selects.forEach(selId => {
        const sel = document.getElementById(selId);
        if (!sel) return;
        sel.innerHTML = '';
        topics.forEach(t => {
            const opt = document.createElement('option');
            opt.value = t.topic;
            opt.textContent = t.topic;
            if (t.topic === appState.selectedTopic) {
                opt.selected = true;
            }
            sel.appendChild(opt);
        });
        if (appState.selectedTopic) {
            sel.value = appState.selectedTopic;
        }
    });

    updatePartitionSelects(appState.selectedTopic);
    updateTargetBadge();
}

function updatePartitionSelects(topicName) {
    const topic = (appState.topics || []).find(t => t.topic === topicName);
    const pCount = topic && topic.partitions && topic.partitions.length > 0 ? topic.partitions.length : 3;

    // 1. Producer Partition Select
    const prodSel = document.getElementById('prodPartitionSelect');
    if (prodSel) {
        const prevVal = prodSel.value;
        prodSel.innerHTML = '<option value="auto">✨ Auto (Key Hash / Round-Robin)</option>';
        for (let i = 0; i < pCount; i++) {
            const opt = document.createElement('option');
            opt.value = String(i);
            opt.textContent = `Partition ${i}`;
            if (prevVal === String(i)) opt.selected = true;
            prodSel.appendChild(opt);
        }
        if (prevVal === 'auto' || !prevVal) prodSel.value = 'auto';
    }

    // 2. Inspector Partition Select
    const inspSel = document.getElementById('inspectPartSelect');
    if (inspSel) {
        const prevVal = inspSel.value;
        inspSel.innerHTML = '';
        for (let i = 0; i < pCount; i++) {
            const opt = document.createElement('option');
            opt.value = String(i);
            opt.textContent = `Partition ${i}`;
            if (prevVal === String(i)) opt.selected = true;
            inspSel.appendChild(opt);
        }
        if (!inspSel.value && inspSel.options.length > 0) inspSel.selectedIndex = 0;
    }

    // 3. Rebalance consumer cluster for this topic
    rebalanceAndRenderConsumers();
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
        card.addEventListener('click', async () => {
            appState.selectedTopic = t.topic;
            document.querySelectorAll('.topic-card').forEach(c => c.classList.remove('active'));
            card.classList.add('active');
            const prodSel = document.getElementById('prodTopicSelect');
            if (prodSel) prodSel.value = t.topic;
            const consSel = document.getElementById('consTopicSelect');
            if (consSel) consSel.value = t.topic;
            const inspSel = document.getElementById('inspectTopicSelect');
            if (inspSel) inspSel.value = t.topic;
            await syncCommittedOffsets(appState.activeGroupId, t.topic);
            updatePartitionSelects(t.topic);
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
// Produce Message (Supports Auto Routing)
// ==========================================

async function handleProduceSubmit(e) {
    e.preventDefault();
    const topic = document.getElementById('prodTopicSelect').value;
    const partSelectVal = document.getElementById('prodPartitionSelect').value;
    const key = document.getElementById('prodKeyInput').value.trim();
    const value = document.getElementById('prodValueInput').value.trim();

    if (!topic) {
        alert("Please select or create a topic first.");
        return;
    }

    const submitBtn = document.getElementById('produceSubmitBtn');
    submitBtn.disabled = true;
    submitBtn.textContent = "WRITING TO COMMIT LOG...";

    // Determine target partition
    const topicObj = (appState.topics || []).find(t => t.topic === topic);
    const pCount = topicObj && topicObj.partitions && topicObj.partitions.length > 0 ? topicObj.partitions.length : 3;
    let partition = -1;
    let routeMode = 'Manual';

    if (partSelectVal === 'auto') {
        if (key) {
            let hash = 0;
            for (let i = 0; i < key.length; i++) {
                hash = ((hash << 5) - hash) + key.charCodeAt(i);
                hash |= 0;
            }
            partition = Math.abs(hash) % pCount;
            routeMode = `Auto (Key Hash)`;
        } else {
            partition = Math.abs(appState.clientRoundRobinSeq++) % pCount;
            routeMode = `Auto (Round-Robin)`;
        }
    } else {
        partition = parseInt(partSelectVal);
        routeMode = `Manual (Part ${partition})`;
    }

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
            document.getElementById('rcptPart').textContent = `${data.topic}:${data.partition} [${routeMode}]`;
            document.getElementById('rcptCrc').textContent = data.crc;
            document.getElementById('rcptTs').textContent = `${new Date(data.timestamp).toLocaleTimeString()} (${latency}ms)`;

            // Flash receipt
            receipt.style.transform = 'scale(1.02)';
            setTimeout(() => { receipt.style.transform = 'scale(1)'; }, 150);

            // Refresh topics
            await loadTopics();
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
// Consumer Group Cluster & Partition Assignment
// ==========================================

function getOrCreateGroup(groupId) {
    if (!appState.consumerGroups[groupId]) {
        appState.consumerGroups[groupId] = {
            strategy: 'auto-range',
            consumers: [
                {
                    id: 'c-1',
                    name: 'Consumer #1',
                    manualPartitions: [],
                    assignedPartitions: [],
                    offsets: {},
                    polledRecords: [],
                    autoPollInterval: null
                }
            ]
        };
    }
    return appState.consumerGroups[groupId];
}

async function switchConsumerGroup(groupId) {
    if (!groupId) return;
    
    // Stop any active auto-poll timers on the previous group to prevent background polling
    const prevGroup = appState.consumerGroups[appState.activeGroupId];
    if (prevGroup && prevGroup.consumers) {
        prevGroup.consumers.forEach(c => {
            if (c.autoPollInterval) {
                clearInterval(c.autoPollInterval);
                c.autoPollInterval = null;
            }
        });
    }

    appState.activeGroupId = groupId;
    const input = document.getElementById('consGroupInput');
    if (input) input.value = groupId;

    getOrCreateGroup(groupId);
    renderGroupChips();
    await syncCommittedOffsets(groupId, appState.selectedTopic);
    rebalanceAndRenderConsumers();
}

function renderGroupChips() {
    const container = document.getElementById('groupChipsContainer');
    if (!container) return;
    container.innerHTML = '';
    const groupIds = Object.keys(appState.consumerGroups);
    groupIds.forEach(gid => {
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = `chip group-chip ${gid === appState.activeGroupId ? 'active' : ''}`;
        chip.setAttribute('data-group', gid);
        chip.textContent = gid;
        chip.addEventListener('click', () => switchConsumerGroup(gid));
        container.appendChild(chip);
    });
}

function openCreateGroupModal() {
    const modal = document.getElementById('createGroupModal');
    if (modal) {
        modal.classList.remove('hidden');
        const input = document.getElementById('newGroupName');
        if (input) {
            input.value = '';
            setTimeout(() => input.focus(), 50);
        }
    }
}

function closeCreateGroupModal() {
    const modal = document.getElementById('createGroupModal');
    if (modal) modal.classList.add('hidden');
}

function handleCreateGroup(e) {
    e.preventDefault();
    const nameInput = document.getElementById('newGroupName');
    const instancesInput = document.getElementById('newGroupInstances');
    const groupName = nameInput.value.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '-');
    const instances = Math.min(8, Math.max(1, parseInt(instancesInput.value) || 1));

    if (!groupName) {
        alert("Please enter a valid group ID.");
        return;
    }

    if (!appState.consumerGroups[groupName]) {
        const consumers = [];
        for (let i = 1; i <= instances; i++) {
            consumers.push({
                id: `c-${i}`,
                name: `Consumer #${i}`,
                manualPartitions: [],
                assignedPartitions: [],
                offsets: {},
                polledRecords: [],
                autoPollInterval: null
            });
        }
        appState.consumerGroups[groupName] = {
            strategy: 'auto-range',
            consumers: consumers
        };
    }

    closeCreateGroupModal();
    switchConsumerGroup(groupName);
}

function setAssignmentStrategy(strategy) {
    const group = getOrCreateGroup(appState.activeGroupId);
    if (strategy === 'manual' && group.strategy !== 'manual') {
        // Seamless transition: initialize manualPartitions from current assignments with strict 1-to-1 mapping
        const claimed = new Set();
        group.consumers.forEach(c => {
            c.manualPartitions = (c.assignedPartitions || []).filter(p => {
                if (!claimed.has(p)) {
                    claimed.add(p);
                    return true;
                }
                return false;
            });
        });
    }
    group.strategy = strategy;
    rebalanceAndRenderConsumers();
}

async function addConsumerInstance() {
    const group = getOrCreateGroup(appState.activeGroupId);
    const newIdx = group.consumers.length + 1;
    group.consumers.push({
        id: `c-${Date.now().toString(36).substring(4)}`,
        name: `Consumer #${newIdx}`,
        manualPartitions: [],
        assignedPartitions: [],
        offsets: {},
        polledRecords: [],
        autoPollInterval: null
    });
    await syncCommittedOffsets(appState.activeGroupId, appState.selectedTopic);
    rebalanceAndRenderConsumers();
}

function removeConsumerInstance(consumerId) {
    const group = getOrCreateGroup(appState.activeGroupId);
    if (group.consumers.length <= 1) return;
    const target = group.consumers.find(c => c.id === consumerId);
    if (target && target.autoPollInterval) {
        clearInterval(target.autoPollInterval);
        target.autoPollInterval = null;
    }
    group.consumers = group.consumers.filter(c => c.id !== consumerId);
    rebalanceAndRenderConsumers();
}

function rebalanceAndRenderConsumers() {
    const groupId = appState.activeGroupId || 'analytics-workers';
    const group = getOrCreateGroup(groupId);
    const topic = (appState.topics || []).find(t => t.topic === appState.selectedTopic) || { partitions: [{ partition: 0, highWatermark: 0 }] };
    const pCount = Math.max(1, (topic.partitions || []).length);
    const consumers = group.consumers;
    const cCount = Math.max(1, consumers.length);

    // 1. Calculate partition assignments according to strategy
    if (group.strategy === 'auto-range') {
        const base = Math.floor(pCount / cCount);
        const remainder = pCount % cCount;
        let pIndex = 0;
        for (let i = 0; i < cCount; i++) {
            const count = base + (i < remainder ? 1 : 0);
            consumers[i].assignedPartitions = [];
            for (let j = 0; j < count; j++) {
                consumers[i].assignedPartitions.push(pIndex++);
            }
        }
    } else if (group.strategy === 'auto-rr') {
        consumers.forEach(c => c.assignedPartitions = []);
        for (let p = 0; p < pCount; p++) {
            consumers[p % cCount].assignedPartitions.push(p);
        }
    } else if (group.strategy === 'manual') {
        // Enforce strict Kafka invariant: Each partition can belong to AT MOST ONE consumer instance in the same group!
        const claimedPartitions = new Set();
        consumers.forEach(c => {
            c.manualPartitions = (c.manualPartitions || []).filter(p => {
                if (p < pCount && !claimedPartitions.has(p)) {
                    claimedPartitions.add(p);
                    return true;
                }
                return false;
            });
            c.assignedPartitions = [...c.manualPartitions];
        });
    }

    // 2. Update Header Badges & Strategy Buttons
    const badgeCount = document.getElementById('consumerGroupCountBadge');
    if (badgeCount) badgeCount.textContent = `${cCount} CONSUMER${cCount > 1 ? 'S' : ''}`;
    const badgePos = document.getElementById('consumerPosBadge');
    if (badgePos) badgePos.textContent = `GROUP: ${groupId}`;

    const stratRange = document.getElementById('stratRangeBtn');
    const stratRR = document.getElementById('stratRoundRobinBtn');
    const stratManual = document.getElementById('stratManualBtn');
    if (stratRange) stratRange.className = `btn btn-xs ${group.strategy === 'auto-range' ? 'btn-yellow active' : 'btn-white'}`;
    if (stratRR) stratRR.className = `btn btn-xs ${group.strategy === 'auto-rr' ? 'btn-yellow active' : 'btn-white'}`;
    if (stratManual) stratManual.className = `btn btn-xs ${group.strategy === 'manual' ? 'btn-yellow active' : 'btn-white'}`;

    // Synchronize toolbar Auto Poll button state
    const autoAllBtn = document.getElementById('toggleAutoPollAllBtn');
    if (autoAllBtn) {
        const isAnyRunning = consumers.some(c => !!c.autoPollInterval);
        if (isAnyRunning) {
            autoAllBtn.textContent = 'AUTO: ON (1.2s)';
            autoAllBtn.className = 'btn btn-sm btn-lime';
        } else {
            autoAllBtn.textContent = 'AUTO: OFF';
            autoAllBtn.className = 'btn btn-sm btn-white';
        }
    }

    // 3. Render Consumer Instance Cards
    const container = document.getElementById('consumerInstancesContainer');
    if (!container) return;
    container.innerHTML = '';

    consumers.forEach(c => {
        const card = document.createElement('div');
        card.className = 'consumer-instance-card';
        card.id = `card-${c.id}`;

        const isAutoRunning = !!c.autoPollInterval;
        let assignedHtml = '';
        if (group.strategy === 'manual') {
            for (let p = 0; p < pCount; p++) {
                const isChecked = c.assignedPartitions.includes(p);
                // Check if another consumer in this group has already claimed partition p
                const owner = group.consumers.find(other => other.id !== c.id && (other.assignedPartitions || []).includes(p));
                const isDisabled = !isChecked && !!owner;
                const tooltip = isDisabled 
                    ? `Partition ${p} is already claimed by ${owner.name}. In a Kafka consumer group, a partition can only be assigned to 1 consumer!` 
                    : isChecked 
                        ? `Partition ${p} is assigned to ${c.name}. Uncheck to release.` 
                        : `Click to assign Partition ${p} exclusively to ${c.name}.`;

                const ownerBadge = isDisabled ? ` <span class="owner-tag">(${escapeHtml(owner.name.replace('Consumer #', 'C'))})</span>` : '';

                assignedHtml += `
                    <label class="part-chk-label ${isDisabled ? 'disabled' : ''} ${isChecked ? 'active-chk' : ''}" title="${tooltip}">
                        <input type="checkbox" class="manual-part-chk" data-cid="${c.id}" data-part="${p}" 
                            ${isChecked ? 'checked' : ''} 
                            ${isDisabled ? 'disabled' : ''}>
                        Part ${p}${ownerBadge}
                    </label>
                `;
            }
        } else {
            if (c.assignedPartitions.length === 0) {
                assignedHtml = `<span class="dim text-xs">Idle (no partitions assigned)</span>`;
            } else {
                c.assignedPartitions.forEach(p => {
                    assignedHtml += `<span class="pill-badge pill-yellow">Part ${p}</span>`;
                });
            }
        }

        // Lag & Partition detail rows
        let lagRowsHtml = '';
        if (c.assignedPartitions.length === 0) {
            lagRowsHtml = `<div class="empty-state text-xs" style="padding:6px;">No partitions assigned to this worker. Add partitions or adjust strategy.</div>`;
        } else {
            c.assignedPartitions.forEach(p => {
                const partMeta = (topic.partitions || []).find(item => item.partition === p) || { highWatermark: 0 };
                const currentPos = (c.offsets[p] !== undefined) ? c.offsets[p] : 0;
                const hwm = partMeta.highWatermark || 0;
                const lag = Math.max(0, hwm - currentPos);
                const lagClass = lag > 0 ? 'lag-badge has-lag' : 'lag-badge';

                lagRowsHtml += `
                    <div class="part-lag-item">
                        <span><strong>Part ${p}</strong></span>
                        <span>
                            <span title="Consumer Read Offset (Next message this worker will fetch)">POS: <strong class="highlight">${currentPos}</strong></span>
                            <span class="dim"> | </span>
                            <span title="Broker Disk High Watermark (Total messages committed to log on disk)">HWM: ${hwm}</span>
                        </span>
                        <span class="${lagClass}" title="${lag > 0 ? `${lag} unread messages on disk waiting to be polled` : 'Consumer caught up'}">LAG: ${lag}</span>
                        <div class="part-seek-box">
                            <input type="number" min="0" max="${hwm}" class="part-seek-input" data-cid="${c.id}" data-part="${p}" id="seek-${c.id}-${p}" value="${currentPos}" title="Seek offset: 0 to ${hwm} (HWM)">
                            <button type="button" class="btn btn-xs btn-black part-seek-btn" data-cid="${c.id}" data-part="${p}">SEEK</button>
                        </div>
                    </div>
                `;
            });
        }

        // Records list
        const polledCount = (c.polledRecords || []).length;
        let recordsHtml = '';
        if (polledCount === 0) {
            recordsHtml = `<div class="empty-state text-xs">No records polled yet. Click [POLL (NEXT 10)] above.</div>`;
        } else {
            c.polledRecords.slice(0, 15).forEach(m => {
                recordsHtml += `
                    <div class="instance-record-item">
                        <div class="record-content-box">
                            <div class="record-header-meta">
                                <span class="pill-badge pill-black" style="font-size:0.68rem;">P${m.partition !== undefined ? m.partition : '-'} : #${m.offset}</span>
                                ${m.key ? `<span class="pill-badge pill-yellow" style="font-size:0.65rem;">KEY: ${escapeHtml(m.key)}</span>` : ''}
                            </div>
                            <div class="mono record-val">${escapeHtml(m.value)}</div>
                        </div>
                        <span class="dim" style="font-size:0.68rem; white-space:nowrap;">${new Date(m.timestamp).toLocaleTimeString()}</span>
                    </div>
                `;
            });
        }

        card.innerHTML = `
            <div class="consumer-card-header">
                <div class="consumer-identity">
                    <span class="consumer-status-dot ${isAutoRunning ? '' : 'idle'}"></span>
                    <span class="pill-badge pill-black">${c.name}</span>
                    <span class="text-xs dim mono">${c.id}</span>
                    <span class="pill-badge pill-lime" style="font-size:0.65rem;" title="Offsets are automatically committed to disk on every poll & seek">AUTO-COMMIT</span>
                </div>
                <div class="consumer-card-actions">
                    <button type="button" class="btn btn-xs btn-cyan poll-instance-btn" data-cid="${c.id}">POLL (NEXT 10)</button>
                    <button type="button" class="btn btn-xs ${isAutoRunning ? 'btn-lime' : 'btn-white'} auto-instance-btn" data-cid="${c.id}">${isAutoRunning ? 'AUTO: ON' : 'AUTO: OFF'}</button>
                    ${consumers.length > 1 ? `<button type="button" class="btn btn-xs btn-pink remove-instance-btn" data-cid="${c.id}" title="Remove Consumer">&times;</button>` : ''}
                </div>
            </div>

            <div class="assigned-partitions-bar">
                <span class="text-xs">ASSIGNED PARTITIONS:</span>
                <div class="partition-assignment-chips">
                    ${assignedHtml}
                </div>
            </div>

            <div class="partition-lag-row">
                ${lagRowsHtml}
            </div>

            <div class="instance-feed-container">
                <div class="feed-header">
                    <span class="text-xs">POLLED RECORDS (${polledCount})</span>
                    <button type="button" class="clean-link clear-instance-feed-btn" data-cid="${c.id}" style="font-size:0.75rem;">Clear</button>
                </div>
                <div class="instance-records-list">
                    ${recordsHtml}
                </div>
            </div>
        `;

        container.appendChild(card);
    });

    wireConsumerCardEvents();
}

function wireConsumerCardEvents() {
    // Poll buttons
    document.querySelectorAll('.poll-instance-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const cid = btn.getAttribute('data-cid');
            pollConsumerInstance(cid, 10);
        });
    });


    // Auto buttons
    document.querySelectorAll('.auto-instance-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const cid = btn.getAttribute('data-cid');
            toggleAutoPollConsumer(cid);
        });
    });

    // Remove buttons
    document.querySelectorAll('.remove-instance-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const cid = btn.getAttribute('data-cid');
            removeConsumerInstance(cid);
        });
    });

    // Seek buttons & Enter key
    document.querySelectorAll('.part-seek-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            e.preventDefault();
            const cid = btn.getAttribute('data-cid');
            const part = parseInt(btn.getAttribute('data-part'));
            const input = document.getElementById(`seek-${cid}-${part}`);
            if (input) {
                const val = parseInt(input.value);
                seekConsumerPartition(cid, part, isNaN(val) ? 0 : val);
            }
        });
    });

    document.querySelectorAll('.part-seek-input').forEach(input => {
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                e.stopPropagation();
                const cid = input.getAttribute('data-cid');
                const part = parseInt(input.getAttribute('data-part'));
                const val = parseInt(input.value);
                seekConsumerPartition(cid, part, isNaN(val) ? 0 : val);
            }
        });
    });

    // Manual partition checkboxes
    document.querySelectorAll('.manual-part-chk').forEach(chk => {
        chk.addEventListener('change', () => {
            const cid = chk.getAttribute('data-cid');
            const part = parseInt(chk.getAttribute('data-part'));
            const group = getOrCreateGroup(appState.activeGroupId);
            const consumer = group.consumers.find(c => c.id === cid);
            if (consumer) {
                if (!consumer.manualPartitions) consumer.manualPartitions = [];
                if (chk.checked) {
                    // In Kafka, a partition belongs exclusively to at most 1 consumer instance in a group!
                    // Disassociate this partition from any other consumer in the group if claimed
                    group.consumers.forEach(other => {
                        if (other.id !== cid && other.manualPartitions) {
                            other.manualPartitions = other.manualPartitions.filter(p => p !== part);
                            other.assignedPartitions = (other.assignedPartitions || []).filter(p => p !== part);
                        }
                    });
                    if (!consumer.manualPartitions.includes(part)) {
                        consumer.manualPartitions.push(part);
                    }
                } else {
                    consumer.manualPartitions = consumer.manualPartitions.filter(p => p !== part);
                }
                rebalanceAndRenderConsumers();
            }
        });
    });

    // Clear feed buttons
    document.querySelectorAll('.clear-instance-feed-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const cid = btn.getAttribute('data-cid');
            const group = getOrCreateGroup(appState.activeGroupId);
            const consumer = group.consumers.find(c => c.id === cid);
            if (consumer) {
                consumer.polledRecords = [];
                rebalanceAndRenderConsumers();
            }
        });
    });
}

async function pollConsumerInstance(consumerId, maxTotal = 10) {
    const groupId = appState.activeGroupId;
    const group = getOrCreateGroup(groupId);
    const consumer = group.consumers.find(c => c.id === consumerId);
    if (!consumer || !consumer.assignedPartitions || consumer.assignedPartitions.length === 0) return;

    const topic = appState.selectedTopic;
    let remainingBudget = maxTotal;

    for (const part of consumer.assignedPartitions) {
        if (remainingBudget <= 0) break;

        let offset = consumer.offsets[part];
        if (offset === undefined) {
            // Fetch committed offset from broker
            try {
                const cRes = await fetch(`${API_BASE}/api/commit?groupId=${encodeURIComponent(groupId)}&topic=${encodeURIComponent(topic)}`);
                const cData = await cRes.json();
                offset = (cData.offsets && cData.offsets[part] !== undefined && cData.offsets[part] >= 0) ? cData.offsets[part] : 0;
            } catch (e) {
                offset = 0;
            }
            consumer.offsets[part] = offset;
        }

        try {
            const fetchLimit = remainingBudget;
            const res = await fetch(`${API_BASE}/api/consume?topic=${encodeURIComponent(topic)}&partition=${part}&offset=${offset}&limit=${fetchLimit}`);
            const data = await res.json();
            if (data.messages && data.messages.length > 0) {
                data.messages.forEach(m => {
                    consumer.polledRecords.unshift({ ...m, partition: part });
                });
                consumer.offsets[part] = data.nextOffset;
                remainingBudget -= data.messages.length;

                // Automatic offset commit directly to broker storage (__consumer_offsets.dat)
                try {
                    await fetch(`${API_BASE}/api/commit`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ groupId, topic, partition: part, offset: data.nextOffset })
                    });
                } catch (commitErr) {
                    console.error(`Auto-commit error for consumer ${consumerId} partition ${part}:`, commitErr);
                }
            }
        } catch (e) {
            console.error(`Poll error for consumer ${consumerId} partition ${part}:`, e);
        }
    }

    rebalanceAndRenderConsumers();
}

async function pollAllConsumers() {
    const group = getOrCreateGroup(appState.activeGroupId);
    for (const c of group.consumers) {
        await pollConsumerInstance(c.id, 10);
    }
}

async function seekConsumerPartition(consumerId, partition, newOffset) {
    const groupId = appState.activeGroupId;
    const group = getOrCreateGroup(groupId);
    const consumer = group.consumers.find(c => c.id === consumerId);
    if (!consumer) return;

    // Retrieve Partition metadata to check High Watermark
    const topic = appState.selectedTopic;
    const topicMeta = (appState.topics || []).find(t => t.topic === topic);
    const partMeta = topicMeta && topicMeta.partitions ? topicMeta.partitions.find(p => p.partition === partition) : null;
    const hwm = partMeta ? (partMeta.highWatermark || 0) : 0;
    const currentOffset = consumer.offsets[partition] !== undefined ? consumer.offsets[partition] : 0;

    // Strictly enforce bounds: cannot seek less than 0 or greater than HWM
    if (isNaN(newOffset) || newOffset < 0) {
        openSeekModal('lower', partition, newOffset, hwm, currentOffset);
        const input = document.getElementById(`seek-${consumerId}-${partition}`);
        if (input) input.value = currentOffset;
        return;
    }

    if (newOffset > hwm) {
        openSeekModal('higher', partition, newOffset, hwm, currentOffset);
        const input = document.getElementById(`seek-${consumerId}-${partition}`);
        if (input) input.value = currentOffset;
        return;
    }

    // Stop auto-poll if active so it does not immediately consume from new seek position
    if (consumer.autoPollInterval) {
        clearInterval(consumer.autoPollInterval);
        consumer.autoPollInterval = null;
    }

    // 1. Set local in-memory consumer offset pointer
    consumer.offsets[partition] = newOffset;

    // 2. Automatically commit this offset to server storage (__consumer_offsets.dat)
    try {
        const cRes = await fetch(`${API_BASE}/api/commit`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ groupId, topic, partition, offset: newOffset })
        });
        const cData = await cRes.json();
        if (!cRes.ok || cData.error) {
            alert(`Seek commit error: ${cData.error || 'Unknown error'}`);
            consumer.offsets[partition] = currentOffset;
            rebalanceAndRenderConsumers();
            return;
        }
    } catch (e) {
        console.error("Auto-commit on seek failed:", e);
    }

    // 3. Immediately rebalance and re-render UI to update POS and recalculate LAG (strictly NO polling)
    rebalanceAndRenderConsumers();

    // 4. Visual confirmation on the SEEK button
    const card = document.getElementById(`card-${consumerId}`);
    if (card) {
        const btn = card.querySelector(`.part-seek-btn[data-part="${partition}"]`);
        if (btn) {
            const orig = btn.textContent;
            btn.textContent = 'SEEKED!';
            btn.style.background = '#00F0FF';
            btn.style.color = '#000';
            setTimeout(() => {
                btn.textContent = orig;
                btn.style.background = '';
                btn.style.color = '';
            }, 800);
        }
    }
}

function openSeekModal(type, partition, attemptedOffset, hwm, currentOffset) {
    const modal = document.getElementById('seekWarningModal');
    if (!modal) return;

    const title = document.getElementById('seekModalTitle');
    const msg = document.getElementById('seekModalMessage');
    const details = document.getElementById('seekModalDetails');

    if (type === 'lower') {
        if (title) title.innerHTML = `<span>⚠️</span> CANNOT SEEK BELOW 0`;
        if (msg) msg.textContent = `Attempted seek to offset ${attemptedOffset} is invalid. Offsets cannot be negative.`;
        if (details) {
            details.innerHTML = `
                <div><strong>Target:</strong> Partition ${partition}</div>
                <div><strong>Attempted Offset:</strong> <span style="color:var(--color-pink); font-weight:bold;">${attemptedOffset}</span> (Below 0)</div>
                <div><strong>Valid Offset Range:</strong> 0 to #${hwm} (High Watermark)</div>
                <div><strong>Current Position:</strong> #${currentOffset}</div>
                <div><strong>Status:</strong> Offset reverted to current position.</div>
            `;
        }
    } else {
        if (title) title.innerHTML = `<span>⚠️</span> SEEK EXCEEDS HIGH WATERMARK`;
        if (msg) msg.textContent = `Attempted seek to offset #${attemptedOffset} exceeds the High Watermark (#${hwm}) on Partition ${partition}.`;
        if (details) {
            const overflow = attemptedOffset - hwm;
            details.innerHTML = `
                <div><strong>Target:</strong> Partition ${partition}</div>
                <div><strong>Attempted Offset:</strong> <span style="color:var(--color-pink); font-weight:bold;">#${attemptedOffset}</span></div>
                <div><strong>Partition High Watermark:</strong> #${hwm} (Latest committed message)</div>
                <div><strong>Log Boundary Overflow:</strong> +${overflow} unwritten message(s)</div>
                <div><strong>Valid Offset Range:</strong> 0 to #${hwm}</div>
                <div><strong>Status:</strong> Offset reverted to current position.</div>
            `;
        }
    }

    modal.classList.remove('hidden');
}

function closeSeekModal() {
    const modal = document.getElementById('seekWarningModal');
    if (modal) modal.classList.add('hidden');
}

function toggleAutoPollConsumer(consumerId) {
    const group = getOrCreateGroup(appState.activeGroupId);
    const consumer = group.consumers.find(c => c.id === consumerId);
    if (!consumer) return;

    if (consumer.autoPollInterval) {
        clearInterval(consumer.autoPollInterval);
        consumer.autoPollInterval = null;
    } else {
        consumer.autoPollInterval = setInterval(() => {
            pollConsumerInstance(consumerId, 5);
        }, 1200);
    }
    rebalanceAndRenderConsumers();
}

function toggleAutoPollAll() {
    const group = getOrCreateGroup(appState.activeGroupId);
    const btn = document.getElementById('toggleAutoPollAllBtn');
    const isAnyRunning = group.consumers.some(c => !!c.autoPollInterval);

    if (isAnyRunning) {
        group.consumers.forEach(c => {
            if (c.autoPollInterval) {
                clearInterval(c.autoPollInterval);
                c.autoPollInterval = null;
            }
        });
        if (btn) {
            btn.textContent = 'AUTO: OFF';
            btn.classList.remove('btn-lime');
            btn.classList.add('btn-white');
        }
    } else {
        group.consumers.forEach(c => {
            c.autoPollInterval = setInterval(() => {
                pollConsumerInstance(c.id, 5);
            }, 1200);
        });
        if (btn) {
            btn.textContent = 'AUTO: ON (1.2s)';
            btn.classList.remove('btn-white');
            btn.classList.add('btn-lime');
        }
    }
    rebalanceAndRenderConsumers();
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
    if (!step) return;
    document.getElementById('detailStepNum').textContent = step.num;
    document.getElementById('detailStepTitle').textContent = step.title;
    
    const descEl = document.getElementById('detailStepDesc');
    if (descEl) descEl.textContent = step.desc || '';
    
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
