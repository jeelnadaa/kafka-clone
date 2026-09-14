package broker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import model.Message;
import model.Protocol;
import model.Protocol.*;
import storage.CommitLog;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unified Broker Server.
 * Runs:
 * 1. High-throughput TCP Server (Port 9092) for Kafka Protocol clients.
 * 2. Embedded Zero-Dependency HTTP Server (Port 8080) for the Neo-Brutalism Web Dashboard and REST API.
 */
public class BrokerServer implements Closeable {

    private final int tcpPort;
    private final int httpPort;
    private final File dataDir;
    private final int defaultPartitions;
    private final int maxSegmentBytes;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger serverRoundRobinSeq = new AtomicInteger(0);
    private ServerSocket serverSocket;
    private ExecutorService clientThreadPool;
    private TopicRegistry topicRegistry;
    private Thread acceptThread;
    private HttpServer httpServer;
    private final long startTimeMs = System.currentTimeMillis();

    public BrokerServer(int tcpPort, int httpPort, File dataDir) {
        this(tcpPort, httpPort, dataDir, 3, 1024 * 1024); // 3 partitions, 1MB segments
    }

    public BrokerServer(int tcpPort, int httpPort, File dataDir, int defaultPartitions, int maxSegmentBytes) {
        this.tcpPort = tcpPort;
        this.httpPort = httpPort;
        this.dataDir = dataDir;
        this.defaultPartitions = defaultPartitions;
        this.maxSegmentBytes = maxSegmentBytes;
    }

    public synchronized void start() throws IOException {
        if (running.get()) return;

        this.topicRegistry = new TopicRegistry(dataDir, defaultPartitions, maxSegmentBytes);
        this.clientThreadPool = Executors.newCachedThreadPool();

        // 1. Start TCP Server
        this.serverSocket = new ServerSocket();
        this.serverSocket.setReuseAddress(true);
        this.serverSocket.bind(new InetSocketAddress("0.0.0.0", tcpPort));

        this.running.set(true);
        this.acceptThread = new Thread(this::tcpAcceptLoop, "broker-tcp-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();

        // 2. Start Embedded HTTP Web Dashboard
        if (httpPort > 0) {
            try {
                startHttpServer();
            } catch (Exception e) {
                System.err.println("Note: HTTP server could not bind to port " + httpPort + ": " + e.getMessage());
            }
        }

        System.out.println("=================================================");
        System.out.println("   KAFKA CLONE BROKER RUNNING");
        System.out.println("   TCP Wire Port:  " + getTcpPort());
        if (httpServer != null) {
            System.out.println("   Web Dashboard:  http://localhost:" + httpPort);
        }
        System.out.println("   Data Directory: " + dataDir.getAbsolutePath());
        System.out.println("=================================================");
    }

    private void tcpAcceptLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                socket.setKeepAlive(true);
                clientThreadPool.submit(() -> handleClientSocket(socket));
            } catch (IOException e) {
                if (!running.get()) break;
            }
        }
    }

    private void handleClientSocket(Socket socket) {
        try (InputStream in = new BufferedInputStream(socket.getInputStream());
             OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            while (running.get() && !socket.isClosed()) {
                byte[] frame = Protocol.readFrame(in);
                if (frame == null) break; // Client disconnected

                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(frame));
                byte apiKey = dis.readByte();
                short apiVersion = dis.readShort();
                int correlationId = dis.readInt();

                byte[] responsePayload;
                switch (apiKey) {
                    case Protocol.API_PRODUCE:
                        responsePayload = handleProduce(dis, correlationId);
                        break;
                    case Protocol.API_FETCH:
                        responsePayload = handleFetch(dis, correlationId);
                        break;
                    case Protocol.API_METADATA:
                        responsePayload = handleMetadata(dis, correlationId);
                        break;
                    case Protocol.API_OFFSET_COMMIT:
                        responsePayload = handleOffsetCommit(dis, correlationId);
                        break;
                    case Protocol.API_OFFSET_FETCH:
                        responsePayload = handleOffsetFetch(dis, correlationId);
                        break;
                    default:
                        responsePayload = Protocol.encodeSimpleErrorResponse(correlationId, ErrorCode.SERVER_ERROR);
                        break;
                }

                Protocol.writeFrame(out, responsePayload);
            }
        } catch (Exception ignored) {
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private byte[] handleProduce(DataInputStream in, int corrId) throws IOException {
        ProduceRequest req = Protocol.decodeProduceRequest(in, corrId);
        CommitLog log = topicRegistry.getPartitionLog(req.topic, req.partition);
        if (log == null) {
            return Protocol.encodeSimpleErrorResponse(corrId, ErrorCode.UNKNOWN_PARTITION);
        }

        long baseOffset = log.append(req.messages);
        ProduceResponse resp = new ProduceResponse(
                corrId,
                ErrorCode.NONE,
                req.topic,
                req.partition,
                baseOffset,
                System.currentTimeMillis()
        );
        return Protocol.encodeProduceResponse(resp);
    }

    private byte[] handleFetch(DataInputStream in, int corrId) throws IOException {
        FetchRequest req = Protocol.decodeFetchRequest(in, corrId);
        CommitLog log = topicRegistry.getPartitionLog(req.topic, req.partition);
        if (log == null) {
            return Protocol.encodeSimpleErrorResponse(corrId, ErrorCode.UNKNOWN_PARTITION);
        }

        List<Message> messages = log.read(req.fetchOffset, req.maxMessages, req.maxBytes);
        FetchResponse resp = new FetchResponse(
                corrId,
                ErrorCode.NONE,
                req.topic,
                req.partition,
                messages,
                log.getHighWatermark()
        );
        return Protocol.encodeFetchResponse(resp);
    }

    private byte[] handleMetadata(DataInputStream in, int corrId) throws IOException {
        int topicCount = in.readInt();
        Set<String> topicsToQuery = new HashSet<>();
        if (topicCount > 0) {
            for (int i = 0; i < topicCount; i++) {
                topicsToQuery.add(Protocol.readString(in));
            }
        } else {
            topicsToQuery.addAll(topicRegistry.getTopicNames());
        }

        List<MetadataTopicInfo> topicInfos = new ArrayList<>();
        for (String topic : topicsToQuery) {
            int partitionCount = topicRegistry.getPartitionCount(topic);
            List<Long> hwms = new ArrayList<>();
            for (int p = 0; p < partitionCount; p++) {
                CommitLog log = topicRegistry.getPartitionLog(topic, p);
                hwms.add(log != null ? log.getHighWatermark() : 0L);
            }
            topicInfos.add(new MetadataTopicInfo(topic, partitionCount, hwms));
        }

        MetadataResponse resp = new MetadataResponse(corrId, ErrorCode.NONE, topicInfos);
        return Protocol.encodeMetadataResponse(resp);
    }

    private byte[] handleOffsetCommit(DataInputStream in, int corrId) throws IOException {
        String groupId = Protocol.readString(in);
        String topic = Protocol.readString(in);
        int partition = in.readInt();
        long offset = in.readLong();

        topicRegistry.commitOffset(groupId, topic, partition, offset);
        return Protocol.encodeSimpleErrorResponse(corrId, ErrorCode.NONE);
    }

    private byte[] handleOffsetFetch(DataInputStream in, int corrId) throws IOException {
        String groupId = Protocol.readString(in);
        String topic = Protocol.readString(in);
        int partition = in.readInt();

        long committed = topicRegistry.fetchCommittedOffset(groupId, topic, partition);
        return Protocol.encodeOffsetFetchResponse(corrId, ErrorCode.NONE, committed);
    }

    // ==========================================
    // Embedded HTTP Dashboard & REST API
    // ==========================================

    private void startHttpServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);

        // API Endpoints
        httpServer.createContext("/api/status", this::handleApiStatus);
        httpServer.createContext("/api/topics", this::handleApiTopics);
        httpServer.createContext("/api/produce", this::handleApiProduce);
        httpServer.createContext("/api/consume", this::handleApiConsume);
        httpServer.createContext("/api/commit", this::handleApiCommit);
        httpServer.createContext("/api/create-topic", this::handleApiCreateTopic);
        httpServer.createContext("/api/log-viewer", this::handleApiLogViewer);
        httpServer.createContext("/api/docs", this::handleApiDocs);

        // Static files handler (Neo-Brutalism frontend)
        httpServer.createContext("/", this::handleStaticFiles);

        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.start();
    }

    private void handleApiStatus(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        long uptimeSec = (System.currentTimeMillis() - startTimeMs) / 1000;
        int topicCount = topicRegistry.getTopicNames().size();
        long totalMessages = 0;
        for (String topic : topicRegistry.getTopicNames()) {
            int pCount = topicRegistry.getPartitionCount(topic);
            for (int i = 0; i < pCount; i++) {
                CommitLog log = topicRegistry.getPartitionLog(topic, i);
                if (log != null) totalMessages += log.getHighWatermark();
            }
        }

        String json = "{"
                + "\"status\":\"ONLINE\","
                + "\"tcpPort\":" + getTcpPort() + ","
                + "\"httpPort\":" + httpPort + ","
                + "\"uptimeSeconds\":" + uptimeSec + ","
                + "\"topicsCount\":" + topicCount + ","
                + "\"totalMessages\":" + totalMessages + ","
                + "\"dataDir\":\"" + escapeJson(dataDir.getAbsolutePath()) + "\""
                + "}";
        sendJsonResponse(exchange, 200, json);
    }

    private void handleApiTopics(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        StringBuilder sb = new StringBuilder("[");
        boolean firstTopic = true;
        for (String topic : topicRegistry.getTopicNames()) {
            if (!firstTopic) sb.append(",");
            firstTopic = false;

            int pCount = topicRegistry.getPartitionCount(topic);
            sb.append("{\"topic\":\"").append(escapeJson(topic)).append("\",\"partitions\":[");
            for (int p = 0; p < pCount; p++) {
                if (p > 0) sb.append(",");
                CommitLog log = topicRegistry.getPartitionLog(topic, p);
                long hwm = (log != null) ? log.getHighWatermark() : 0L;
                int segCount = (log != null) ? log.getSegments().size() : 0;
                sb.append("{\"partition\":").append(p)
                        .append(",\"highWatermark\":").append(hwm)
                        .append(",\"segmentsCount\":").append(segCount).append("}");
            }
            sb.append("]}");
        }
        sb.append("]");
        sendJsonResponse(exchange, 200, sb.toString());
    }

    private void handleApiProduce(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJsonResponse(exchange, 405, "{\"error\":\"Method Not Allowed\"}");
            return;
        }

        String body = readRequestBody(exchange);
        String topic = extractJsonField(body, "topic");
        String key = extractJsonField(body, "key");
        String value = extractJsonField(body, "value");

        if (topic == null || topic.isEmpty()) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Missing topic\"}");
            return;
        }

        int pCount = topicRegistry.getPartitionCount(topic);
        if (pCount <= 0) pCount = defaultPartitions;

        // Auto partition routing if partition is omitted, negative, or "auto"
        int partition = extractJsonInt(body, "partition", -1);
        String partStr = extractJsonField(body, "partition");
        if (partition < 0 || "auto".equalsIgnoreCase(partStr) || (!body.contains("\"partition\""))) {
            if (key != null && !key.trim().isEmpty()) {
                partition = Math.abs(key.hashCode()) % pCount;
            } else {
                partition = Math.abs(serverRoundRobinSeq.getAndIncrement()) % pCount;
            }
        }

        CommitLog log = topicRegistry.getPartitionLog(topic, partition);
        if (log == null) {
            sendJsonResponse(exchange, 404, "{\"error\":\"Partition not found\"}");
            return;
        }

        Message msg = Message.of(key, value);
        long offset = log.append(Collections.singletonList(msg));

        String json = "{"
                + "\"success\":true,"
                + "\"topic\":\"" + escapeJson(topic) + "\","
                + "\"partition\":" + partition + ","
                + "\"offset\":" + offset + ","
                + "\"timestamp\":" + msg.getTimestamp() + ","
                + "\"crc\":" + msg.getCrc()
                + "}";
        sendJsonResponse(exchange, 200, json);
    }

    private void handleApiConsume(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String topic = params.get("topic");
        int partition = Integer.parseInt(params.getOrDefault("partition", "0"));
        long offset = Long.parseLong(params.getOrDefault("offset", "0"));
        int limit = Integer.parseInt(params.getOrDefault("limit", "20"));

        if (topic == null) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Missing topic param\"}");
            return;
        }

        CommitLog log = topicRegistry.getPartitionLog(topic, partition);
        if (log == null) {
            sendJsonResponse(exchange, 404, "{\"error\":\"Topic/partition not found\"}");
            return;
        }

        List<Message> messages = log.read(offset, limit, 5 * 1024 * 1024);
        long nextOffset = offset;
        if (!messages.isEmpty()) {
            nextOffset = messages.get(messages.size() - 1).getOffset() + 1;
        }
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"topic\":\"").append(escapeJson(topic)).append("\",");
        sb.append("\"partition\":").append(partition).append(",");
        sb.append("\"nextOffset\":").append(nextOffset).append(",");
        sb.append("\"highWatermark\":").append(log.getHighWatermark()).append(",");
        sb.append("\"messages\":[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) sb.append(",");
            Message m = messages.get(i);
            sb.append("{")
                    .append("\"offset\":").append(m.getOffset()).append(",")
                    .append("\"timestamp\":").append(m.getTimestamp()).append(",")
                    .append("\"key\":").append(m.getKey() != null ? "\"" + escapeJson(m.getKeyAsString()) + "\"" : "null").append(",")
                    .append("\"value\":\"").append(escapeJson(m.getValueAsString())).append("\",")
                    .append("\"crc\":").append(m.getCrc())
                    .append("}");
        }
        sb.append("]}");
        sendJsonResponse(exchange, 200, sb.toString());
    }

    private void handleApiCommit(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
            String groupId = params.get("groupId");
            String topic = params.get("topic");

            if (groupId == null || topic == null) {
                sendJsonResponse(exchange, 400, "{\"error\":\"Missing groupId or topic parameter\"}");
                return;
            }

            int pCount = topicRegistry.getPartitionCount(topic);
            StringBuilder sb = new StringBuilder("{");
            sb.append("\"groupId\":\"").append(escapeJson(groupId)).append("\",");
            sb.append("\"topic\":\"").append(escapeJson(topic)).append("\",");
            sb.append("\"offsets\":{");
            for (int p = 0; p < pCount; p++) {
                if (p > 0) sb.append(",");
                long committed = topicRegistry.fetchCommittedOffset(groupId, topic, p);
                sb.append("\"").append(p).append("\":").append(committed);
            }
            sb.append("}}");
            sendJsonResponse(exchange, 200, sb.toString());
            return;
        }

        String body = readRequestBody(exchange);
        String groupId = extractJsonField(body, "groupId");
        String topic = extractJsonField(body, "topic");
        int partition = extractJsonInt(body, "partition", 0);
        long offset = extractJsonLong(body, "offset", 0L);

        if (groupId == null || topic == null) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Missing groupId or topic\"}");
            return;
        }

        storage.CommitLog pLog = topicRegistry.getPartitionLog(topic, partition);
        if (pLog != null) {
            long hwm = pLog.getHighWatermark();
            if (offset < 0 || offset > hwm) {
                sendJsonResponse(exchange, 400, "{\"error\":\"Seek offset " + offset + " is out of bounds (0.." + hwm + ")\"}");
                return;
            }
        }

        topicRegistry.commitOffset(groupId, topic, partition, offset);
        sendJsonResponse(exchange, 200, "{\"success\":true,\"committedOffset\":" + offset + "}");
    }

    private void handleApiCreateTopic(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        String body = readRequestBody(exchange);
        String topic = extractJsonField(body, "topic");
        int partitions = extractJsonInt(body, "partitions", 3);

        if (topic == null || topic.trim().isEmpty()) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Topic name cannot be empty\"}");
            return;
        }

        topicRegistry.getOrCreateTopic(topic.trim(), partitions);
        sendJsonResponse(exchange, 200, "{\"success\":true,\"topic\":\"" + escapeJson(topic.trim()) + "\",\"partitions\":" + partitions + "}");
    }

    private void handleApiLogViewer(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String topic = params.get("topic");
        int partition = Integer.parseInt(params.getOrDefault("partition", "0"));

        if (topic == null) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Missing topic\"}");
            return;
        }

        CommitLog log = topicRegistry.getPartitionLog(topic, partition);
        if (log == null) {
            sendJsonResponse(exchange, 404, "{\"error\":\"Topic/partition not found\"}");
            return;
        }

        StringBuilder sb = new StringBuilder("{");
        sb.append("\"topic\":\"").append(escapeJson(topic)).append("\",");
        sb.append("\"partition\":").append(partition).append(",");
        sb.append("\"partitionDir\":\"").append(escapeJson(log.getPartitionDir().getAbsolutePath())).append("\",");
        sb.append("\"highWatermark\":").append(log.getHighWatermark()).append(",");
        sb.append("\"segments\":[");

        boolean firstSeg = true;
        for (CommitLog.Segment seg : log.getSegments()) {
            if (!firstSeg) sb.append(",");
            firstSeg = false;
            sb.append("{")
                    .append("\"baseOffset\":").append(seg.getBaseOffset()).append(",")
                    .append("\"logFile\":\"").append(escapeJson(seg.getLogFile().getName())).append("\",")
                    .append("\"logSizeBytes\":").append(seg.getCurrentSize()).append(",")
                    .append("\"indexEntriesCount\":").append(seg.getIndex().getEntriesCount())
                    .append("}");
        }
        sb.append("],");

        // Include last 50 messages for visual inspection
        List<Message> sample = log.read(0, 50, 1024 * 1024);
        sb.append("\"messages\":[");
        for (int i = 0; i < sample.size(); i++) {
            if (i > 0) sb.append(",");
            Message m = sample.get(i);
            sb.append("{")
                    .append("\"offset\":").append(m.getOffset()).append(",")
                    .append("\"timestamp\":").append(m.getTimestamp()).append(",")
                    .append("\"key\":").append(m.getKey() != null ? "\"" + escapeJson(m.getKeyAsString()) + "\"" : "null").append(",")
                    .append("\"value\":\"").append(escapeJson(m.getValueAsString())).append("\",")
                    .append("\"crc\":").append(m.getCrc())
                    .append("}");
        }
        sb.append("]}");

        sendJsonResponse(exchange, 200, sb.toString());
    }

    private void handleApiDocs(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
        String name = params.getOrDefault("name", "readme").toLowerCase();

        File docFile;
        String title;
        if ("testing".equals(name)) {
            docFile = findDocFile("MANUAL_TESTING_GUIDE.md");
            title = "MANUAL_TESTING_GUIDE.md";
        } else if ("architecture".equals(name) || "pipeline".equals(name)) {
            docFile = findDocFile("docs/ARCHITECTURE_AND_PIPELINE.md");
            title = "ARCHITECTURE_AND_PIPELINE.md";
        } else {
            docFile = findDocFile("README.md");
            title = "README.md";
        }

        String content = "";
        if (docFile != null && docFile.exists()) {
            content = java.nio.file.Files.readString(docFile.toPath(), StandardCharsets.UTF_8);
        } else {
            content = "# Document Not Found: " + title;
        }

        String json = "{\"title\":\"" + escapeJson(title) + "\",\"content\":\"" + escapeJson(content) + "\"}";
        sendJsonResponse(exchange, 200, json);
    }

    private File findDocFile(String relativePath) {
        File f1 = new File(relativePath);
        if (f1.exists()) return f1;
        File f2 = new File("cleaned", relativePath);
        if (f2.exists()) return f2;
        return null;
    }

    private void handleStaticFiles(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/") || path.isEmpty()) {
            path = "/index.html";
        }

        // Try serving from "./web" or fallback to "cleaned/web"
        File webDir = new File("web");
        if (!webDir.exists()) {
            webDir = new File("cleaned/web"); // replace cleaned with your folder name.
        }
        File target = new File(webDir, path.startsWith("/") ? path.substring(1) : path);

        if (!target.exists() || target.isDirectory()) {
            sendJsonResponse(exchange, 404, "File Not Found: " + path);
            return;
        }

        String mime = "text/plain";
        if (path.endsWith(".html")) mime = "text/html; charset=UTF-8";
        else if (path.endsWith(".css")) mime = "text/css; charset=UTF-8";
        else if (path.endsWith(".js")) mime = "application/javascript; charset=UTF-8";
        else if (path.endsWith(".svg")) mime = "image/svg+xml";

        exchange.getResponseHeaders().set("Content-Type", mime);
        byte[] bytes = java.nio.file.Files.readAllBytes(target.toPath());
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    // ==========================================
    // HTTP Helpers
    // ==========================================

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private void sendJsonResponse(HttpExchange exchange, int code, String json) throws IOException {
        addCorsHeaders(exchange);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String readRequestBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null) return map;
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2) {
                map.put(pair[0], pair[1]);
            }
        }
        return map;
    }

    private String extractJsonField(String json, String field) {
        if (json == null) return null;
        String searchKey = "\"" + field + "\"";
        int keyIdx = json.indexOf(searchKey);
        if (keyIdx == -1) return null;
        int colonIdx = json.indexOf(':', keyIdx + searchKey.length());
        if (colonIdx == -1) return null;

        // Skip whitespace after colon
        int valStart = colonIdx + 1;
        while (valStart < json.length() && Character.isWhitespace(json.charAt(valStart))) {
            valStart++;
        }
        if (valStart >= json.length()) return null;

        char firstChar = json.charAt(valStart);
        if (firstChar == '"') {
            // Quoted string: read until closing unescaped quote and unescape characters
            StringBuilder sb = new StringBuilder();
            boolean escape = false;
            for (int i = valStart + 1; i < json.length(); i++) {
                char c = json.charAt(i);
                if (escape) {
                    switch (c) {
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        default -> sb.append(c);
                    }
                    escape = false;
                } else if (c == '\\') {
                    escape = true;
                } else if (c == '"') {
                    return sb.toString();
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        } else if (firstChar == '{' || firstChar == '[') {
            // Raw JSON object/array: extract by balancing delimiters
            char open = firstChar;
            char close = (open == '{') ? '}' : ']';
            int depth = 0;
            boolean inStr = false;
            boolean esc = false;
            for (int i = valStart; i < json.length(); i++) {
                char c = json.charAt(i);
                if (esc) {
                    esc = false;
                } else if (c == '\\') {
                    esc = true;
                } else if (c == '"') {
                    inStr = !inStr;
                } else if (!inStr) {
                    if (c == open) depth++;
                    else if (c == close) {
                        depth--;
                        if (depth == 0) return json.substring(valStart, i + 1);
                    }
                }
            }
        } else {
            // Primitive (numbers, booleans, null)
            int end = valStart;
            while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}' && !Character.isWhitespace(json.charAt(end))) {
                end++;
            }
            return json.substring(valStart, end);
        }
        return null;
    }

    private int extractJsonInt(String json, String field, int defaultVal) {
        if (json == null) return defaultVal;
        String pattern = "\"" + field + "\"\\s*:\\s*([0-9]+)";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern).matcher(json);
        if (m.find()) return Integer.parseInt(m.group(1));
        return defaultVal;
    }

    private long extractJsonLong(String json, String field, long defaultVal) {
        if (json == null) return defaultVal;
        String pattern = "\"" + field + "\"\\s*:\\s*([0-9]+)";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern).matcher(json);
        if (m.find()) return Long.parseLong(m.group(1));
        return defaultVal;
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    public int getTcpPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : tcpPort;
    }

    public TopicRegistry getTopicRegistry() {
        return topicRegistry;
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) return;
        System.out.println("Stopping Kafka Clone Broker...");

        if (httpServer != null) {
            httpServer.stop(0);
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}

        if (clientThreadPool != null) {
            clientThreadPool.shutdownNow();
        }

        if (topicRegistry != null) {
            try { topicRegistry.close(); } catch (IOException ignored) {}
        }
    }

    @Override
    public void close() {
        stop();
    }

    public static void main(String[] args) throws Exception {
        int tcpPort = args.length > 0 ? Integer.parseInt(args[0]) : 9092;
        int httpPort = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        File dataDir = args.length > 2 ? new File(args[2]) : new File("./kafka_data");

        BrokerServer broker = new BrokerServer(tcpPort, httpPort, dataDir);
        broker.start();

        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

        // Keep process running
        Thread.currentThread().join();
    }
}
