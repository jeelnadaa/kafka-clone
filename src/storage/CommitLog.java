package storage;

import model.Message;
import model.Protocol;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append-only Commit Log Storage Engine for a single partition.
 *
 * Persists messages sequentially to disk in segmented .log files alongside .index files.
 * Provides O(1) sequential appends and logarithmic indexed lookups.
 */
public class CommitLog implements Closeable {

    private final File partitionDir;
    private final int maxSegmentBytes;
    private final ConcurrentSkipListMap<Long, Segment> segments = new ConcurrentSkipListMap<>();
    private final AtomicLong nextOffset = new AtomicLong(0L);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Segment activeSegment;

    public CommitLog(File partitionDir, int maxSegmentBytes) throws IOException {
        this.partitionDir = partitionDir;
        this.maxSegmentBytes = maxSegmentBytes;
        if (!partitionDir.exists()) {
            partitionDir.mkdirs();
        }
        recoverAndInitialize();
    }

    /**
     * Inner class representing a paired (.log, .index) segment.
     */
    public static class Segment implements Closeable {
        private final long baseOffset;
        private final File logFile;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final OffsetIndex index;

        public Segment(File partitionDir, long baseOffset) throws IOException {
            this.baseOffset = baseOffset;
            String prefix = String.format("%020d", baseOffset);
            this.logFile = new File(partitionDir, prefix + ".log");
            File idxFile = new File(partitionDir, prefix + ".index");

            this.raf = new RandomAccessFile(logFile, "rw");
            this.channel = raf.getChannel();
            this.index = new OffsetIndex(idxFile);
        }

        public synchronized void append(Message msg) throws IOException {
            long currentPos = channel.position();
            // Record offset mapping in index
            index.append(msg.getOffset(), currentPos);

            byte[] serialized = Protocol.serializeMessage(msg);
            channel.write(ByteBuffer.wrap(serialized));
        }

        public List<Message> read(long fromOffset, int maxMessages, int maxBytes) throws IOException {
            List<Message> results = new ArrayList<>();
            long startPos = index.lookup(fromOffset);

            long fileSize = channel.size();
            if (startPos >= fileSize) {
                return results;
            }

            int toRead = (int) Math.min(fileSize - startPos, (long) maxBytes);
            ByteBuffer buf = ByteBuffer.allocate(Math.max(toRead, 4096));
            channel.position(startPos);
            channel.read(buf);
            buf.flip();

            int bytesAccumulated = 0;
            while (buf.remaining() >= 28 && results.size() < maxMessages) { // Minimum message size: 28 bytes
                buf.mark();
                long offset = buf.getLong();
                long ts = buf.getLong();
                int keyLen = buf.getInt();
                if (keyLen > 0 && buf.remaining() < keyLen) { buf.reset(); break; }
                byte[] key = null;
                if (keyLen >= 0) {
                    key = new byte[keyLen];
                    buf.get(key);
                }

                if (buf.remaining() < 4) { buf.reset(); break; }
                int valLen = buf.getInt();
                if (valLen > 0 && buf.remaining() < valLen) { buf.reset(); break; }
                byte[] val = null;
                if (valLen >= 0) {
                    val = new byte[valLen];
                    buf.get(val);
                }

                if (buf.remaining() < 4) { buf.reset(); break; }
                long crc = buf.getInt() & 0xFFFFFFFFL;

                Message msg = new Message(offset, ts, key, val, crc);
                if (offset >= fromOffset) {
                    results.add(msg);
                    bytesAccumulated += (28 + (keyLen > 0 ? keyLen : 0) + (valLen > 0 ? valLen : 0));
                    if (bytesAccumulated >= maxBytes) {
                        break;
                    }
                }
            }
            return results;
        }

        public long getBaseOffset() { return baseOffset; }
        public long getCurrentSize() throws IOException { return channel.size(); }
        public OffsetIndex getIndex() { return index; }
        public File getLogFile() { return logFile; }

        public synchronized void flush() throws IOException {
            if (channel.isOpen()) channel.force(true);
            index.flush();
        }

        @Override
        public synchronized void close() throws IOException {
            flush();
            channel.close();
            raf.close();
            index.close();
        }
    }

    private void recoverAndInitialize() throws IOException {
        File[] logFiles = partitionDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (logFiles != null && logFiles.length > 0) {
            Arrays.sort(logFiles, Comparator.comparing(File::getName));
            long highestOffset = -1L;

            for (File file : logFiles) {
                String name = file.getName();
                long base = Long.parseLong(name.substring(0, name.indexOf(".log")));
                Segment segment = new Segment(partitionDir, base);
                segments.put(base, segment);

                // Scan messages to find highest offset
                List<Message> sample = segment.read(base, Integer.MAX_VALUE, Integer.MAX_VALUE);
                for (Message m : sample) {
                    if (m.getOffset() > highestOffset) {
                        highestOffset = m.getOffset();
                    }
                }
            }

            activeSegment = segments.lastEntry().getValue();
            nextOffset.set(highestOffset + 1);
        } else {
            // First time initialization: segment at offset 0
            roll(0L);
            nextOffset.set(0L);
        }
    }

    private void roll(long newBaseOffset) throws IOException {
        if (activeSegment != null) {
            activeSegment.flush();
        }
        Segment newSegment = new Segment(partitionDir, newBaseOffset);
        segments.put(newBaseOffset, newSegment);
        activeSegment = newSegment;
    }

    /**
     * Appends a batch of messages to the commit log.
     * Returns the base offset assigned to the first message in the batch.
     */
    public long append(List<Message> messages) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return nextOffset.get();
        }

        rwLock.writeLock().lock();
        try {
            long baseOffset = nextOffset.get();
            for (Message original : messages) {
                if (activeSegment.getCurrentSize() >= maxSegmentBytes) {
                    roll(nextOffset.get());
                }
                long assignedOffset = nextOffset.getAndIncrement();
                Message msgWithOffset = original.withOffset(assignedOffset);
                activeSegment.append(msgWithOffset);
            }
            activeSegment.flush();
            return baseOffset;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Reads messages starting from startOffset up to limits.
     */
    public List<Message> read(long startOffset, int maxMessages, int maxBytes) throws IOException {
        rwLock.readLock().lock();
        try {
            if (startOffset >= nextOffset.get()) {
                return Collections.emptyList();
            }

            // Find segment containing startOffset
            Map.Entry<Long, Segment> entry = segments.floorEntry(startOffset);
            if (entry == null) {
                entry = segments.firstEntry();
            }

            List<Message> results = new ArrayList<>();
            long currentStart = startOffset;

            while (entry != null && results.size() < maxMessages) {
                Segment seg = entry.getValue();
                List<Message> batch = seg.read(currentStart, maxMessages - results.size(), maxBytes);
                results.addAll(batch);

                if (!batch.isEmpty()) {
                    currentStart = batch.get(batch.size() - 1).getOffset() + 1;
                }

                if (results.size() >= maxMessages) {
                    break;
                }
                entry = segments.higherEntry(seg.getBaseOffset());
            }

            return results;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long getHighWatermark() {
        return nextOffset.get();
    }

    public Collection<Segment> getSegments() {
        return segments.values();
    }

    public File getPartitionDir() {
        return partitionDir;
    }

    @Override
    public synchronized void close() throws IOException {
        rwLock.writeLock().lock();
        try {
            for (Segment s : segments.values()) {
                s.close();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
