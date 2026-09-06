package storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Binary Offset Index (.index file).
 *
 * Each entry is exactly 16 bytes:
 * [8 bytes Offset] [8 bytes File Position in .log]
 *
 * Provides O(log N) lookup to find the exact byte position where a message starts.
 */
public class OffsetIndex implements Closeable {

    public static final int ENTRY_SIZE = 16; // 8 bytes offset + 8 bytes position

    private final File indexFile;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final ConcurrentSkipListMap<Long, Long> inMemoryMap = new ConcurrentSkipListMap<>();
    private long entriesCount = 0;

    public OffsetIndex(File indexFile) throws IOException {
        this.indexFile = indexFile;
        File parent = indexFile.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        this.raf = new RandomAccessFile(indexFile, "rw");
        this.channel = raf.getChannel();
        loadExisting();
    }

    private void loadExisting() throws IOException {
        long size = channel.size();
        if (size == 0) return;

        ByteBuffer buf = ByteBuffer.allocate((int) Math.min(size, 4 * 1024 * 1024)); // up to 4MB at a time
        channel.position(0);
        long bytesReadTotal = 0;

        while (bytesReadTotal < size) {
            buf.clear();
            int read = channel.read(buf);
            if (read <= 0) break;
            buf.flip();

            while (buf.remaining() >= ENTRY_SIZE) {
                long offset = buf.getLong();
                long position = buf.getLong();
                inMemoryMap.put(offset, position);
                entriesCount++;
                bytesReadTotal += ENTRY_SIZE;
            }
        }
    }

    /**
     * Appends a new mapping: logical message offset -> physical byte offset in .log.
     */
    public synchronized void append(long offset, long position) throws IOException {
        inMemoryMap.put(offset, position);
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putLong(offset);
        buf.putLong(position);
        buf.flip();

        channel.position(entriesCount * ENTRY_SIZE);
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        entriesCount++;
    }

    /**
     * Finds the largest indexed offset <= targetOffset.
     * Returns the physical byte position in the log file to seek to.
     */
    public long lookup(long targetOffset) {
        Map.Entry<Long, Long> entry = inMemoryMap.floorEntry(targetOffset);
        return (entry != null) ? entry.getValue() : 0L;
    }

    public long getEntriesCount() {
        return entriesCount;
    }

    public File getFile() {
        return indexFile;
    }

    public synchronized void flush() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.force(true);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            flush();
            channel.close();
            raf.close();
        }
    }
}
