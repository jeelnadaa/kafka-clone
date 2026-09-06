package model;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Represents a single message unit stored in the commit log and sent across the wire.
 *
 * Wire & Storage Format:
 * [8 bytes Offset] [8 bytes Timestamp] [4 bytes Key Length] [Key Bytes] [4 bytes Value Length] [Value Bytes] [4 bytes CRC32]
 */
public class Message {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;
    private final long crc;

    public Message(long offset, long timestamp, byte[] key, byte[] value, long crc) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.crc = crc;
    }

    /**
     * Factory method for creating a new message before it has been assigned a storage offset.
     */
    public static Message of(String key, String value) {
        byte[] k = (key != null) ? key.getBytes(StandardCharsets.UTF_8) : null;
        byte[] v = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
        long now = System.currentTimeMillis();
        long calculatedCrc = computeChecksum(now, k, v);
        return new Message(-1L, now, k, v, calculatedCrc);
    }

    public static Message of(byte[] key, byte[] value) {
        long now = System.currentTimeMillis();
        long calculatedCrc = computeChecksum(now, key, value);
        return new Message(-1L, now, key, value, calculatedCrc);
    }

    /**
     * Clones the message with an assigned sequential offset.
     */
    public Message withOffset(long assignedOffset) {
        return new Message(assignedOffset, this.timestamp, this.key, this.value, this.crc);
    }

    public static long computeChecksum(long timestamp, byte[] key, byte[] value) {
        CRC32 crc32 = new CRC32();
        // Update with timestamp
        for (int i = 7; i >= 0; i--) {
            crc32.update((int) ((timestamp >>> (i * 8)) & 0xFF));
        }
        // Update with key
        if (key != null) {
            crc32.update(key);
        }
        // Update with value
        if (value != null) {
            crc32.update(value);
        }
        return crc32.getValue();
    }

    public boolean isChecksumValid() {
        return this.crc == computeChecksum(this.timestamp, this.key, this.value);
    }

    public long getOffset() { return offset; }
    public long getTimestamp() { return timestamp; }
    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public long getCrc() { return crc; }

    public String getKeyAsString() {
        return key != null ? new String(key, StandardCharsets.UTF_8) : null;
    }

    public String getValueAsString() {
        return value != null ? new String(value, StandardCharsets.UTF_8) : "";
    }

    @Override
    public String toString() {
        return "Message{" +
                "offset=" + offset +
                ", ts=" + timestamp +
                ", key='" + getKeyAsString() + '\'' +
                ", value='" + getValueAsString() + '\'' +
                ", crc=" + crc +
                '}';
    }
}
