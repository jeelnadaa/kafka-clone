package model;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unified protocol codec for framing and message serialization.
 *
 * Wire Framing:
 * [4-byte total payload length] [payload]
 *
 * Request Header:
 * [1-byte apiKey] [2-byte apiVersion] [4-byte correlationId]
 *
 * Response Header:
 * [4-byte correlationId] [2-byte errorCode]
 */
public final class Protocol {

    public static final short API_VERSION = 1;

    // API Keys
    public static final byte API_PRODUCE = 1;
    public static final byte API_FETCH = 2;
    public static final byte API_METADATA = 3;
    public static final byte API_OFFSET_COMMIT = 4;
    public static final byte API_OFFSET_FETCH = 5;

    // Error Codes
    public enum ErrorCode {
        NONE((short) 0, "No error"),
        UNKNOWN_TOPIC((short) 1, "Topic does not exist"),
        OFFSET_OUT_OF_BOUNDS((short) 2, "Offset out of range"),
        CORRUPT_MESSAGE((short) 3, "CRC checksum failed or message corrupt"),
        UNKNOWN_PARTITION((short) 4, "Partition does not exist"),
        SERVER_ERROR((short) 5, "Internal broker server error");

        private final short code;
        private final String description;

        ErrorCode(short code, String description) {
            this.code = code;
            this.description = description;
        }

        public short getCode() { return code; }
        public String getDescription() { return description; }

        public static ErrorCode fromCode(short code) {
            for (ErrorCode e : values()) {
                if (e.code == code) return e;
            }
            return SERVER_ERROR;
        }
    }

    // ==========================================
    // Request & Response Records
    // ==========================================

    public static class ProduceRequest {
        public final int correlationId;
        public final String topic;
        public final int partition;
        public final short acks;
        public final int timeoutMs;
        public final List<Message> messages;

        public ProduceRequest(int correlationId, String topic, int partition, short acks, int timeoutMs, List<Message> messages) {
            this.correlationId = correlationId;
            this.topic = topic;
            this.partition = partition;
            this.acks = acks;
            this.timeoutMs = timeoutMs;
            this.messages = messages;
        }
    }

    public static class ProduceResponse {
        public final int correlationId;
        public final ErrorCode errorCode;
        public final String topic;
        public final int partition;
        public final long baseOffset;
        public final long logAppendTimeMs;

        public ProduceResponse(int correlationId, ErrorCode errorCode, String topic, int partition, long baseOffset, long logAppendTimeMs) {
            this.correlationId = correlationId;
            this.errorCode = errorCode;
            this.topic = topic;
            this.partition = partition;
            this.baseOffset = baseOffset;
            this.logAppendTimeMs = logAppendTimeMs;
        }
    }

    public static class FetchRequest {
        public final int correlationId;
        public final String topic;
        public final int partition;
        public final long fetchOffset;
        public final int maxMessages;
        public final int maxBytes;

        public FetchRequest(int correlationId, String topic, int partition, long fetchOffset, int maxMessages, int maxBytes) {
            this.correlationId = correlationId;
            this.topic = topic;
            this.partition = partition;
            this.fetchOffset = fetchOffset;
            this.maxMessages = maxMessages;
            this.maxBytes = maxBytes;
        }
    }

    public static class FetchResponse {
        public final int correlationId;
        public final ErrorCode errorCode;
        public final String topic;
        public final int partition;
        public final List<Message> messages;
        public final long highWatermark;

        public FetchResponse(int correlationId, ErrorCode errorCode, String topic, int partition, List<Message> messages, long highWatermark) {
            this.correlationId = correlationId;
            this.errorCode = errorCode;
            this.topic = topic;
            this.partition = partition;
            this.messages = messages;
            this.highWatermark = highWatermark;
        }
    }

    public static class MetadataTopicInfo {
        public final String topic;
        public final int partitionCount;
        public final List<Long> highWatermarks;

        public MetadataTopicInfo(String topic, int partitionCount, List<Long> highWatermarks) {
            this.topic = topic;
            this.partitionCount = partitionCount;
            this.highWatermarks = highWatermarks;
        }
    }

    public static class MetadataResponse {
        public final int correlationId;
        public final ErrorCode errorCode;
        public final List<MetadataTopicInfo> topics;

        public MetadataResponse(int correlationId, ErrorCode errorCode, List<MetadataTopicInfo> topics) {
            this.correlationId = correlationId;
            this.errorCode = errorCode;
            this.topics = topics;
        }
    }

    // ==========================================
    // Framing Read/Write Helpers
    // ==========================================

    public static byte[] readFrame(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int length;
        try {
            length = dis.readInt();
        } catch (EOFException e) {
            return null; // Connection closed gracefully
        }
        if (length <= 0 || length > 32 * 1024 * 1024) { // 32MB safety bound
            throw new IOException("Invalid frame length: " + length);
        }
        byte[] payload = new byte[length];
        dis.readFully(payload);
        return payload;
    }

    public static void writeFrame(OutputStream out, byte[] payload) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(payload.length);
        dos.write(payload);
        dos.flush();
    }

    public static void writeString(DataOutputStream out, String str) throws IOException {
        if (str == null) {
            out.writeShort(-1);
        } else {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            out.writeShort((short) bytes.length);
            out.write(bytes);
        }
    }

    public static String readString(DataInputStream in) throws IOException {
        short len = in.readShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    public static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    // ==========================================
    // Message Serializer (Binary Log & Wire)
    // ==========================================

    public static byte[] serializeMessage(Message msg) {
        int keyLen = (msg.getKey() != null) ? msg.getKey().length : 0;
        int valLen = (msg.getValue() != null) ? msg.getValue().length : 0;
        // 8 (offset) + 8 (ts) + 4 (keyLen) + keyLen + 4 (valLen) + valLen + 4 (crc)
        ByteBuffer buf = ByteBuffer.allocate(8 + 8 + 4 + keyLen + 4 + valLen + 4);
        buf.putLong(msg.getOffset());
        buf.putLong(msg.getTimestamp());
        buf.putInt(msg.getKey() != null ? keyLen : -1);
        if (msg.getKey() != null) buf.put(msg.getKey());
        buf.putInt(msg.getValue() != null ? valLen : -1);
        if (msg.getValue() != null) buf.put(msg.getValue());
        buf.putInt((int) (msg.getCrc() & 0xFFFFFFFFL));
        return buf.array();
    }

    public static Message deserializeMessage(ByteBuffer buf) {
        long offset = buf.getLong();
        long timestamp = buf.getLong();
        int keyLen = buf.getInt();
        byte[] key = null;
        if (keyLen >= 0) {
            key = new byte[keyLen];
            buf.get(key);
        }
        int valLen = buf.getInt();
        byte[] val = null;
        if (valLen >= 0) {
            val = new byte[valLen];
            buf.get(val);
        }
        long crc = buf.getInt() & 0xFFFFFFFFL;
        return new Message(offset, timestamp, key, val, crc);
    }

    // ==========================================
    // Protocol Encoders & Decoders
    // ==========================================

    // PRODUCE
    public static byte[] encodeProduceRequest(ProduceRequest req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(API_PRODUCE);
        out.writeShort(API_VERSION);
        out.writeInt(req.correlationId);
        writeString(out, req.topic);
        out.writeInt(req.partition);
        out.writeShort(req.acks);
        out.writeInt(req.timeoutMs);
        out.writeInt(req.messages.size());
        for (Message msg : req.messages) {
            byte[] bytes = serializeMessage(msg);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        return baos.toByteArray();
    }

    public static ProduceRequest decodeProduceRequest(DataInputStream in, int correlationId) throws IOException {
        String topic = readString(in);
        int partition = in.readInt();
        short acks = in.readShort();
        int timeoutMs = in.readInt();
        int count = in.readInt();
        List<Message> msgs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = in.readInt();
            byte[] b = new byte[len];
            in.readFully(b);
            msgs.add(deserializeMessage(ByteBuffer.wrap(b)));
        }
        return new ProduceRequest(correlationId, topic, partition, acks, timeoutMs, msgs);
    }

    public static byte[] encodeProduceResponse(ProduceResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(resp.correlationId);
        out.writeShort(resp.errorCode.getCode());
        writeString(out, resp.topic);
        out.writeInt(resp.partition);
        out.writeLong(resp.baseOffset);
        out.writeLong(resp.logAppendTimeMs);
        return baos.toByteArray();
    }

    public static ProduceResponse decodeProduceResponse(DataInputStream in, int correlationId, ErrorCode errorCode) throws IOException {
        String topic = readString(in);
        int partition = in.readInt();
        long baseOffset = in.readLong();
        long logAppendTimeMs = in.readLong();
        return new ProduceResponse(correlationId, errorCode, topic, partition, baseOffset, logAppendTimeMs);
    }

    // FETCH
    public static byte[] encodeFetchRequest(FetchRequest req) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(API_FETCH);
        out.writeShort(API_VERSION);
        out.writeInt(req.correlationId);
        writeString(out, req.topic);
        out.writeInt(req.partition);
        out.writeLong(req.fetchOffset);
        out.writeInt(req.maxMessages);
        out.writeInt(req.maxBytes);
        return baos.toByteArray();
    }

    public static FetchRequest decodeFetchRequest(DataInputStream in, int correlationId) throws IOException {
        String topic = readString(in);
        int partition = in.readInt();
        long offset = in.readLong();
        int maxMessages = in.readInt();
        int maxBytes = in.readInt();
        return new FetchRequest(correlationId, topic, partition, offset, maxMessages, maxBytes);
    }

    public static byte[] encodeFetchResponse(FetchResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(resp.correlationId);
        out.writeShort(resp.errorCode.getCode());
        writeString(out, resp.topic);
        out.writeInt(resp.partition);
        out.writeLong(resp.highWatermark);
        out.writeInt(resp.messages.size());
        for (Message m : resp.messages) {
            byte[] bytes = serializeMessage(m);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        return baos.toByteArray();
    }

    public static FetchResponse decodeFetchResponse(DataInputStream in, int correlationId, ErrorCode errorCode) throws IOException {
        String topic = readString(in);
        int partition = in.readInt();
        long hwm = in.readLong();
        int count = in.readInt();
        List<Message> msgs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = in.readInt();
            byte[] b = new byte[len];
            in.readFully(b);
            msgs.add(deserializeMessage(ByteBuffer.wrap(b)));
        }
        return new FetchResponse(correlationId, errorCode, topic, partition, msgs, hwm);
    }

    // METADATA
    public static byte[] encodeMetadataRequest(int correlationId, List<String> topics) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(API_METADATA);
        out.writeShort(API_VERSION);
        out.writeInt(correlationId);
        out.writeInt(topics != null ? topics.size() : 0);
        if (topics != null) {
            for (String t : topics) writeString(out, t);
        }
        return baos.toByteArray();
    }

    public static byte[] encodeMetadataResponse(MetadataResponse resp) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(resp.correlationId);
        out.writeShort(resp.errorCode.getCode());
        out.writeInt(resp.topics.size());
        for (MetadataTopicInfo t : resp.topics) {
            writeString(out, t.topic);
            out.writeInt(t.partitionCount);
            out.writeInt(t.highWatermarks.size());
            for (Long hwm : t.highWatermarks) {
                out.writeLong(hwm);
            }
        }
        return baos.toByteArray();
    }

    public static MetadataResponse decodeMetadataResponse(DataInputStream in, int correlationId, ErrorCode errorCode) throws IOException {
        int topicCount = in.readInt();
        List<MetadataTopicInfo> list = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String topic = readString(in);
            int partitionCount = in.readInt();
            int hwmCount = in.readInt();
            List<Long> hwms = new ArrayList<>(hwmCount);
            for (int j = 0; j < hwmCount; j++) {
                hwms.add(in.readLong());
            }
            list.add(new MetadataTopicInfo(topic, partitionCount, hwms));
        }
        return new MetadataResponse(correlationId, errorCode, list);
    }

    // OFFSET COMMIT & FETCH
    public static byte[] encodeOffsetCommitRequest(int correlationId, String groupId, String topic, int partition, long offset) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(API_OFFSET_COMMIT);
        out.writeShort(API_VERSION);
        out.writeInt(correlationId);
        writeString(out, groupId);
        writeString(out, topic);
        out.writeInt(partition);
        out.writeLong(offset);
        return baos.toByteArray();
    }

    public static byte[] encodeOffsetFetchRequest(int correlationId, String groupId, String topic, int partition) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeByte(API_OFFSET_FETCH);
        out.writeShort(API_VERSION);
        out.writeInt(correlationId);
        writeString(out, groupId);
        writeString(out, topic);
        out.writeInt(partition);
        return baos.toByteArray();
    }

    public static byte[] encodeOffsetFetchResponse(int correlationId, ErrorCode errorCode, long offset) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(correlationId);
        out.writeShort(errorCode.getCode());
        out.writeLong(offset);
        return baos.toByteArray();
    }

    public static byte[] encodeSimpleErrorResponse(int correlationId, ErrorCode errorCode) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(correlationId);
        out.writeShort(errorCode.getCode());
        return baos.toByteArray();
    }
}
