package client;

import model.Message;
import model.Protocol;
import model.Protocol.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lightweight, thread-safe Producer client.
 * Connects over persistent TCP to the Kafka broker.
 */
public class Producer implements Closeable {

    private final String host;
    private final int port;
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private final AtomicInteger correlationSeq = new AtomicInteger(1);

    public static class RecordMetadata {
        public final String topic;
        public final int partition;
        public final long offset;
        public final long timestamp;

        public RecordMetadata(String topic, int partition, long offset, long timestamp) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "RecordMetadata{topic='" + topic + "', partition=" + partition + ", offset=" + offset + "}";
        }
    }

    public Producer(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private synchronized void connect() throws IOException {
        if (socket != null && !socket.isClosed()) return;
        this.socket = new Socket(host, port);
        this.socket.setTcpNoDelay(true);
        this.socket.setKeepAlive(true);
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    public synchronized RecordMetadata send(String topic, String key, String value) throws IOException {
        // Automatic partition hashing based on key
        int partition = 0;
        if (key != null) {
            partition = Math.abs(key.hashCode()) % 3; // Default 3 partitions
        }
        return send(topic, partition, key, value);
    }

    public synchronized RecordMetadata send(String topic, int partition, String key, String value) throws IOException {
        connect();
        Message msg = Message.of(key, value);
        int corrId = correlationSeq.getAndIncrement();

        ProduceRequest req = new ProduceRequest(
                corrId,
                topic,
                partition,
                (short) 1,
                5000,
                Collections.singletonList(msg)
        );

        byte[] requestPayload = Protocol.encodeProduceRequest(req);
        Protocol.writeFrame(out, requestPayload);

        byte[] responsePayload = Protocol.readFrame(in);
        if (responsePayload == null) {
            throw new IOException("Broker disconnected prematurely");
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(responsePayload));
        int respCorrId = dis.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(dis.readShort());

        if (errorCode != ErrorCode.NONE) {
            throw new IOException("Produce error from broker: " + errorCode + " - " + errorCode.getDescription());
        }

        ProduceResponse resp = Protocol.decodeProduceResponse(dis, respCorrId, errorCode);
        return new RecordMetadata(resp.topic, resp.partition, resp.baseOffset, resp.logAppendTimeMs);
    }

    @Override
    public synchronized void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
