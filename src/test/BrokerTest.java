package test;

import broker.BrokerServer;
import client.Consumer;
import client.Producer;
import client.Producer.RecordMetadata;
import model.Message;

import java.io.File;
import java.util.List;

/**
 * Automated broker integration & reboot recovery test.
 */
public class BrokerTest {

    public static void main(String[] args) {
        System.out.println("==========================================================");
        System.out.println("   KAFKA CLONE: AUTOMATED INTEGRATION & RECOVERY TEST     ");
        System.out.println("==========================================================");

        int tcpPort = 19093;
        int httpPort = 18083;
        File testDir = new File("./test_data/broker_test_" + System.currentTimeMillis());

        BrokerServer broker = null;
        try {
            broker = new BrokerServer(tcpPort, httpPort, testDir, 2, 1024 * 1024);
            broker.start();
            Thread.sleep(300);

            // Step 1: Produce Messages
            System.out.println("\n[1/6] Producing 20 test messages across partitions...");
            try (Producer producer = new Producer("127.0.0.1", tcpPort)) {
                for (int i = 0; i < 20; i++) {
                    RecordMetadata meta = producer.send("orders", 0, "order-" + i, "amount: $" + (100 + i * 5));
                    if (meta.offset != i) {
                        throw new AssertionError("Expected offset " + i + ", but got " + meta.offset);
                    }
                }
            }
            System.out.println("  [PASS] Successfully produced 20 sequential messages (offsets 0..19)");

            // Step 2: Consume Messages
            System.out.println("\n[2/6] Consuming messages via Consumer...");
            try (Consumer consumer = new Consumer("127.0.0.1", tcpPort)) {
                consumer.subscribe("orders", 0, 0L);
                List<Message> messages = consumer.poll(20, 3000);
                if (messages.size() != 20) {
                    throw new AssertionError("Expected 20 records, got " + messages.size());
                }
                for (int i = 0; i < 20; i++) {
                    Message m = messages.get(i);
                    String expectedKey = "order-" + i;
                    String expectedVal = "amount: $" + (100 + i * 5);
                    if (!expectedKey.equals(m.getKeyAsString()) || !expectedVal.equals(m.getValueAsString())) {
                        throw new AssertionError("Payload mismatch at index " + i + ": " + m);
                    }
                    if (!m.isChecksumValid()) {
                        throw new AssertionError("CRC Checksum corruption detected at message " + i);
                    }
                }
                System.out.println("  [PASS] Consumed 20 messages; all payloads and CRCs verified");

                // Step 3: Commit Offset
                System.out.println("\n[3/6] Committing consumer group offset...");
                consumer.commitSync("billing-service-group");
                System.out.println("  [PASS] Committed offset " + consumer.getPosition() + " for group 'billing-service-group'");
            }

            // Step 4: Verify Committed Offset
            System.out.println("\n[4/6] Verifying committed offset retrieval...");
            try (Consumer consumer = new Consumer("127.0.0.1", tcpPort)) {
                consumer.subscribe("orders", 0);
                long committed = consumer.fetchCommittedOffset("billing-service-group");
                if (committed != 20L) {
                    throw new AssertionError("Expected committed offset 20, got " + committed);
                }
                System.out.println("  [PASS] Retrieved committed offset: " + committed);
            }

            // Step 5: Restart Broker Server (Simulate Crash / Recovery)
            System.out.println("\n[5/6] Simulating broker crash & reboot recovery...");
            broker.stop();
            Thread.sleep(600);

            // Re-instantiate broker against the same data directory
            broker = new BrokerServer(tcpPort, httpPort, testDir, 2, 1024 * 1024);
            broker.start();
            Thread.sleep(300);
            System.out.println("  [PASS] Broker rebooted and re-indexed existing disk log segments");

            // Step 6: Verify Persistence and Offset Survival
            System.out.println("\n[6/6] Verifying data and consumer group offset survival...");
            try (Consumer consumer = new Consumer("127.0.0.1", tcpPort)) {
                consumer.subscribe("orders", 0);
                consumer.loadAndSeekCommittedOffset("billing-service-group");
                if (consumer.getPosition() != 20L) {
                    throw new AssertionError("Expected position 20 after seek, got " + consumer.getPosition());
                }

                // Verify historical replay from offset 0
                consumer.seek(0L);
                List<Message> replayed = consumer.poll(20, 3000);
                if (replayed.size() != 20) {
                    throw new AssertionError("Expected 20 records after broker recovery, got " + replayed.size());
                }
                System.out.println("  [PASS] Replayed all 20 historical messages from disk log files");

                // Check committed offset persisted to disk
                long persistedCommit = consumer.fetchCommittedOffset("billing-service-group");
                if (persistedCommit != 20L) {
                    throw new AssertionError("Expected persisted offset 20, got " + persistedCommit);
                }
                System.out.println("  [PASS] Consumer group offset persisted to disk survived reboot");
            }

            System.out.println("\n==========================================================");
            System.out.println("   ALL TESTS PASSED: BROKER RECOVERY VERIFIED             ");
            System.out.println("==========================================================");

        } catch (Throwable t) {
            System.err.println("\n[FAIL] Test failure: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        } finally {
            if (broker != null) {
                broker.stop();
            }
            deleteRecursively(testDir);
        }
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) return;
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
