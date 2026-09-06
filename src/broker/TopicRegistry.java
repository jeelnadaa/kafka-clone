package broker;

import storage.CommitLog;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Topics, Partitions, and Consumer Group Committed Offsets.
 *
 * Persists consumer group offsets into a dedicated offset file (__consumer_offsets.dat)
 * so that consumer group progress is fully retained across broker restarts.
 */
public class TopicRegistry implements Closeable {

    private final File dataDir;
    private final int defaultPartitions;
    private final int maxSegmentBytes;

    // topic -> list of partition CommitLogs
    private final Map<String, List<CommitLog>> topics = new ConcurrentHashMap<>();

    // "groupId:topic:partition" -> committed offset
    private final Map<String, Long> committedOffsets = new ConcurrentHashMap<>();
    private final File offsetsFile;

    public TopicRegistry(File dataDir, int defaultPartitions, int maxSegmentBytes) throws IOException {
        this.dataDir = dataDir;
        this.defaultPartitions = defaultPartitions;
        this.maxSegmentBytes = maxSegmentBytes;
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        this.offsetsFile = new File(dataDir, "__consumer_offsets.dat");
        loadCommittedOffsets();
        discoverExistingTopics();
    }

    private void discoverExistingTopics() throws IOException {
        File[] topicDirs = dataDir.listFiles(File::isDirectory);
        if (topicDirs == null) return;

        for (File dir : topicDirs) {
            String topicName = dir.getName();
            File[] partDirs = dir.listFiles(f -> f.isDirectory() && f.getName().startsWith("partition-"));
            if (partDirs != null && partDirs.length > 0) {
                Arrays.sort(partDirs, Comparator.comparing(File::getName));
                List<CommitLog> partitionLogs = new ArrayList<>();
                for (File pDir : partDirs) {
                    partitionLogs.add(new CommitLog(pDir, maxSegmentBytes));
                }
                topics.put(topicName, partitionLogs);
            }
        }
    }

    public synchronized List<CommitLog> getOrCreateTopic(String topic, int partitionCount) throws IOException {
        int count = partitionCount > 0 ? partitionCount : defaultPartitions;
        List<CommitLog> partitionList = topics.get(topic);
        if (partitionList != null) {
            return partitionList;
        }

        List<CommitLog> newPartitions = new ArrayList<>(count);
        File topicDir = new File(dataDir, topic);
        for (int i = 0; i < count; i++) {
            File pDir = new File(topicDir, "partition-" + i);
            newPartitions.add(new CommitLog(pDir, maxSegmentBytes));
        }
        topics.put(topic, newPartitions);
        return newPartitions;
    }

    public CommitLog getPartitionLog(String topic, int partition) throws IOException {
        List<CommitLog> partitions = getOrCreateTopic(topic, defaultPartitions);
        if (partition < 0 || partition >= partitions.size()) {
            return null;
        }
        return partitions.get(partition);
    }

    public Set<String> getTopicNames() {
        return Collections.unmodifiableSet(topics.keySet());
    }

    public int getPartitionCount(String topic) {
        List<CommitLog> p = topics.get(topic);
        return p != null ? p.size() : defaultPartitions;
    }

    // ==========================================
    // Consumer Group Offset Tracking & Persistence
    // ==========================================

    private String offsetKey(String group, String topic, int partition) {
        return group + ":" + topic + ":" + partition;
    }

    public synchronized void commitOffset(String group, String topic, int partition, long offset) throws IOException {
        committedOffsets.put(offsetKey(group, topic, partition), offset);
        saveCommittedOffsets();
    }

    public long fetchCommittedOffset(String group, String topic, int partition) {
        return committedOffsets.getOrDefault(offsetKey(group, topic, partition), -1L);
    }

    public Map<String, Long> getAllCommittedOffsets() {
        return Collections.unmodifiableMap(committedOffsets);
    }

    private synchronized void saveCommittedOffsets() throws IOException {
        File tempFile = new File(dataDir, "__consumer_offsets.tmp");
        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8))) {
            for (Map.Entry<String, Long> entry : committedOffsets.entrySet()) {
                writer.println(entry.getKey() + "=" + entry.getValue());
            }
        }
        if (offsetsFile.exists()) {
            offsetsFile.delete();
        }
        tempFile.renameTo(offsetsFile);
    }

    private synchronized void loadCommittedOffsets() {
        if (!offsetsFile.exists()) return;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(offsetsFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || !line.contains("=")) continue;
                String[] parts = line.split("=", 2);
                committedOffsets.put(parts[0], Long.parseLong(parts[1]));
            }
        } catch (Exception e) {
            System.err.println("Warning: failed to load committed offsets: " + e.getMessage());
        }
    }

    public File getDataDir() {
        return dataDir;
    }

    @Override
    public synchronized void close() throws IOException {
        saveCommittedOffsets();
        for (List<CommitLog> list : topics.values()) {
            for (CommitLog log : list) {
                log.close();
            }
        }
    }
}
