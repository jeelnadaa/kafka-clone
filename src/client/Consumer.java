package client;

import model.Message;
import model.Protocol;
import model.Protocol.*;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lightweight, sequential Consumer client.
 * Tracks offsets client-side and interacts with broker consumer group offset storage.
 */
public class Consumer implements Closeable {

    private final String host;
    private final int port;
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    private final AtomicInteger correlationSeq = new AtomicInteger(1);

    private String subscribedTopic;
    private int subscribedPartition = 0;
    private long currentOffset = 0L;

    public Consumer(String host, int port) throws IOException {
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

    public void subscribe(String topic, int partition) {
        this.subscribedTopic = topic;
        this.subscribedPartition = partition;
        this.currentOffset = 0L;
    }

    public void subscribe(String topic, int partition, long startOffset) {
        this.subscribedTopic = topic;
        this.subscribedPartition = partition;
        this.currentOffset = startOffset;
    }

    public void seek(long offset) {
        this.currentOffset = offset;
    }

    public long getPosition() {
        return currentOffset;
    }

    public synchronized List<Message> poll(int maxMessages, int timeoutMs) throws IOException {
        if (subscribedTopic == null) {
            throw new IllegalStateException("Consumer is not subscribed. Call subscribe() first.");
        }
        connect();

        int corrId = correlationSeq.getAndIncrement();
        FetchRequest req = new FetchRequest(
                corrId,
                subscribedTopic,
                subscribedPartition,
                currentOffset,
                maxMessages,
                10 * 1024 * 1024 // 10MB
        );

        byte[] requestPayload = Protocol.encodeFetchRequest(req);
        Protocol.writeFrame(out, requestPayload);

        byte[] responsePayload = Protocol.readFrame(in);
        if (responsePayload == null) {
            return Collections.emptyList();
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(responsePayload));
        int respCorrId = dis.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(dis.readShort());

        if (errorCode != ErrorCode.NONE) {
            if (errorCode == ErrorCode.UNKNOWN_TOPIC || errorCode == ErrorCode.UNKNOWN_PARTITION) {
                return Collections.emptyList();
            }
            throw new IOException("Fetch error from broker: " + errorCode + " - " + errorCode.getDescription());
        }

        FetchResponse resp = Protocol.decodeFetchResponse(dis, respCorrId, errorCode);
        List<Message> messages = resp.messages;

        if (!messages.isEmpty()) {
            // Monotonically advance offset to 1 past the highest received message
            currentOffset = messages.get(messages.size() - 1).getOffset() + 1;
        }

        return messages;
    }

    public synchronized void commitSync(String groupId) throws IOException {
        if (subscribedTopic == null) {
            throw new IllegalStateException("Consumer is not subscribed to any topic.");
        }
        connect();

        int corrId = correlationSeq.getAndIncrement();
        byte[] requestPayload = Protocol.encodeOffsetCommitRequest(
                corrId,
                groupId,
                subscribedTopic,
                subscribedPartition,
                currentOffset
        );

        Protocol.writeFrame(out, requestPayload);
        byte[] responsePayload = Protocol.readFrame(in);
        if (responsePayload == null) {
            throw new IOException("Broker disconnected during offset commit");
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(responsePayload));
        int respCorrId = dis.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(dis.readShort());

        if (errorCode != ErrorCode.NONE) {
            throw new IOException("Offset commit failed with code: " + errorCode);
        }
    }

    public synchronized long fetchCommittedOffset(String groupId) throws IOException {
        if (subscribedTopic == null) {
            throw new IllegalStateException("Consumer is not subscribed to any topic.");
        }
        connect();

        int corrId = correlationSeq.getAndIncrement();
        byte[] requestPayload = Protocol.encodeOffsetFetchRequest(
                corrId,
                groupId,
                subscribedTopic,
                subscribedPartition
        );

        Protocol.writeFrame(out, requestPayload);
        byte[] responsePayload = Protocol.readFrame(in);
        if (responsePayload == null) {
            throw new IOException("Broker disconnected during offset fetch");
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(responsePayload));
        int respCorrId = dis.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(dis.readShort());

        if (errorCode != ErrorCode.NONE) {
            throw new IOException("Offset fetch failed with code: " + errorCode);
        }

        return dis.readLong();
    }

    public synchronized void loadAndSeekCommittedOffset(String groupId) throws IOException {
        long committed = fetchCommittedOffset(groupId);
        if (committed >= 0) {
            seek(committed);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
