import glide.api.GlideClusterClient;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.AdvancedGlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

/**
 * Test for deadlock / thread hang issues in async JNI bridge.
 *
 * Simulates production workload:
 * - 60 worker threads with unbounded queue
 * - Synchronous blocking calls to glideClient.del(keys).get()
 * - Multi-key DEL operations (1-3 keys)
 * - 1000ms request/connection timeout
 *
 * Tests two potential hang scenarios when cluster becomes unavailable:
 * 1. Abandoned callbacks - JNI error paths don't complete futures
 * 2. Blocked Tokio runtime - Blocking operations on async runtime threads
 *
 * Monitors for symptoms:
 * - Threads stuck in WAITING on .get()
 * - Queue growing continuously
 * - No tasks completing
 */
public class DeadlockTest {
    // Test configuration
    private static final int WORKER_THREADS = 60;
    private static final int PRODUCER_THREADS = 1;
    //private static final long MESSAGE_RATE_NANOS = 1_000_000;
    private static final long MESSAGE_RATE_NANOS = 250_000;

    // Client configuration
    //private static final int REQUEST_TIMEOUT_MS = 1000;
    private static final int REQUEST_TIMEOUT_MS = Integer.MAX_VALUE;
    private static final int CONNECTION_TIMEOUT_MS = 5000;
    //private static final int INFLIGHT_REQUESTS_LIMIT = 60;
    private static final Integer INFLIGHT_REQUESTS_LIMIT = null;
    private static final boolean REFRESH_TOPOLOGY = true;

    // Runtime state
    private static volatile boolean running = true;
    private static final AtomicLong successCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);
    private static final AtomicLong timeoutCount = new AtomicLong(0);
    private static final AtomicLong maxInflightSeen = new AtomicLong(0);
    private static final AtomicInteger activeThreads = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        String clusterAddresses = System.getProperty("cluster.addresses", "localhost:6379");
        String[] addresses = clusterAddresses.split(",");

        GlideClusterClientConfiguration.GlideClusterClientConfigurationBuilder<?, ?> configBuilder =
            GlideClusterClientConfiguration.builder()
                .requestTimeout(REQUEST_TIMEOUT_MS);

        if (INFLIGHT_REQUESTS_LIMIT != null) {
            configBuilder.inflightRequestsLimit(INFLIGHT_REQUESTS_LIMIT);
        }

        for (String addr : addresses) {
            String[] parts = addr.split(":");
            configBuilder.address(NodeAddress.builder()
                .host(parts[0])
                .port(Integer.parseInt(parts[1]))
                .build());
        }

        AdvancedGlideClusterClientConfiguration.AdvancedGlideClusterClientConfigurationBuilder<?, ?> advancedBuilder =
            AdvancedGlideClusterClientConfiguration.builder()
                .connectionTimeout(CONNECTION_TIMEOUT_MS);

        if (REFRESH_TOPOLOGY) {
            advancedBuilder.refreshTopologyFromInitialNodes(true); // Not available in 2.2.1
        }

        configBuilder.advancedConfiguration(advancedBuilder.build());

        GlideClusterClient client = GlideClusterClient.createClient(configBuilder.build()).get();

        LinkedBlockingQueue<String[]> queue = new LinkedBlockingQueue<>();
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);

        // Start worker threads
        for (int i = 0; i < WORKER_THREADS; i++) {
            executor.submit(() -> workerLoop(client, queue));
        }

        // Start monitor thread
        new Thread(() -> monitorLoop(queue), "monitor").start();

        // Start producer threads
        for (int i = 0; i < PRODUCER_THREADS; i++) {
            final int producerId = i;
            new Thread(() -> producerLoop(queue, producerId), "producer-" + producerId).start();
        }

        // Wait forever (script will kill us after DURATION)
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private static void producerLoop(LinkedBlockingQueue<String[]> queue, int producerId) {
        Random random = new Random();
        int messageCount = 0;

        System.out.println("[PRODUCER-" + producerId + "] Starting producer loop");
        while (running) {
            final int msgId = messageCount++;

            String[] keys = generateCrossSlotKeys(random.nextInt(3) + 1);
            queue.offer(keys);

            if (msgId % 100_000 == 0) {
                System.out.println("[PRODUCER-" + producerId + "] Submitted message " + msgId + ", queue size: " + queue.size());
            }

            try {
                if (MESSAGE_RATE_NANOS > 0) {
                    Thread.sleep(MESSAGE_RATE_NANOS / 1_000_000, (int)(MESSAGE_RATE_NANOS % 1_000_000));
                }
            } catch (InterruptedException e) {
                System.out.println("[PRODUCER-" + producerId + "] Interrupted, exiting");
                Thread.currentThread().interrupt();
                return;
            }
        }
        System.out.println("[PRODUCER] Exiting (running=false)");
    }

    private static void workerLoop(GlideClusterClient client, LinkedBlockingQueue<String[]> queue) {
        long workerId = Thread.currentThread().getId();
        System.out.println("[WORKER-" + workerId + "] Starting worker loop");

        while (true) {
            String[] keys;
            try {
                keys = queue.take();
            } catch (InterruptedException e) {
                System.out.println("[WORKER-" + workerId + "] Interrupted, exiting");
                Thread.currentThread().interrupt();
                return;
            }

            long start = System.nanoTime();
            activeThreads.incrementAndGet();
            try {
                Long deleted = client.del(keys).get();
                long elapsedNs = System.nanoTime() - start;
                successCount.incrementAndGet();

                if (elapsedNs > 2500 * 1_000_000L) {
                    System.out.printf("[WORKER-%d] [DEL SLOW] took %.1fms, deleted=%d%n", workerId, elapsedNs/1e6, deleted);
                }
            } catch (Exception e) {
                errorCount.incrementAndGet();
                String msg = e.getMessage();
                if (e.getCause() != null) {
                    msg = e.getCause().getMessage();
                }

                // Track timeouts
                if (msg != null && (msg.contains("Timeout") || msg.contains("timeout"))) {
                    timeoutCount.incrementAndGet();
                }

                if (msg != null && msg.contains("maximum inflight")) {
                    System.err.println("[WORKER-" + workerId + "] ❌ MAX INFLIGHT REACHED");
                }

                if (errorCount.get() % 100 == 1) {
                    System.err.printf("[WORKER-%d] [DEL ERROR] %.1fms: %s%n", workerId, (System.nanoTime()-start)/1e6, msg);
                }
            } finally {
                activeThreads.decrementAndGet();
            }
        }
    }

    private static String[] generateCrossSlotKeys(int count) {
        String[] keys = new String[count];
        for (int i = 0; i < count; i++) {
            keys[i] = "key:" + System.nanoTime() + ":" + i;
        }
        return keys;
    }

    private static int getInflightCount() {
        try {
            Class<?> registryClass = Class.forName("glide.internal.AsyncRegistry");
            java.lang.reflect.Field field = registryClass.getDeclaredField("activeFutures");
            field.setAccessible(true);
            java.util.Map<?, ?> futures = (java.util.Map<?, ?>) field.get(null);
            return futures.size();
        } catch (Exception e) {
            return -1;
        }
    }

    private static void monitorLoop(LinkedBlockingQueue<String[]> queue) {
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        final long[] lastCompleted = {0};
        final long[] lastTotal = {0};
        final long[] stuckCount = {0};
        final long startTime = System.currentTimeMillis();

        monitor.scheduleAtFixedRate(() -> {
            long queueSize = queue.size();
            int active = activeThreads.get();
            long success = successCount.get();
            long errors = errorCount.get();
            long total = success + errors;
            long memoryMB = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
            int inflight = getInflightCount();

            // Track max inflight seen
            if (inflight > maxInflightSeen.get()) {
                maxInflightSeen.set(inflight);
            }

            long rate = (total - lastTotal[0]) / 2; // 2s interval
            long successRate = (success - lastCompleted[0]) / 2; // Success-only rate
            lastTotal[0] = total;
            lastCompleted[0] = success;

            boolean isStuck = (successRate == 0 && active == WORKER_THREADS && queueSize > 0);
            int stuckThreads = isStuck ? WORKER_THREADS : 0;

            if (isStuck) {
                stuckCount[0]++;
            } else {
                stuckCount[0] = 0;
            }

            long elapsedSec = (System.currentTimeMillis() - startTime) / 1000;
            long timeouts = timeoutCount.get();

            // Report on app metrics
            System.out.printf("[%ds] Total: %d dels | Rate: +%d/s | Errors: %d (TO: %d) | Queue: %d | Active: %d/%d | Stuck: %d | Inflight: %d (max: %d) | Heap: %dMB%n",
                elapsedSec, total, rate, errors, timeouts, queueSize, active, WORKER_THREADS, stuckThreads, inflight, maxInflightSeen.get(), memoryMB
            );

            // Report on stuck threads
            if (stuckCount[0] == 1) {
                System.err.println("");
                System.err.println("❌ THREAD HANG DETECTED: All threads stuck, queue growing");
                System.err.printf("Inflight requests: %d (limit: 60)%n", inflight);
                System.err.printf("Queue depth: %d%n", queueSize);
                System.err.println("Capturing thread dump...");
                System.err.println("");
                for (ThreadInfo ti : tmx.dumpAllThreads(false, false)) {
                    if (ti.getThreadName().contains("pool-") || ti.getThreadState() == Thread.State.WAITING) {
                        System.err.println(ti);
                    }
                }
                System.err.println("");
            }

            // Warn if approaching inflight limit
            if (inflight > 50 && inflight != -1) {
                System.err.printf("⚠️  WARNING: Inflight requests at %d (approaching limit of 60)%n", inflight);
            }

            // Warn on queue growth (OOM risk)
            if (queueSize > 1000) {
                System.err.printf("⚠️  WARNING: Queue depth %d - OOM risk!%n", queueSize);
            }

            // Warn on sustained hang
            if (stuckCount[0] >= 3) {
                System.err.printf("⚠️  WARNING: SUSTAINED HANG for %ds (stuck count: %d)%n", stuckCount[0] * 2, stuckCount[0]);
            }

            if (stuckCount[0] == 0 && lastCompleted[0] > 0 && rate > 0) {
                long prevStuck = stuckThreads;
                if (prevStuck > 0) {
                    System.out.println("✅ RECOVERED: Threads unblocked, rate=" + rate + "/s");
                }
            }
        }, 0, 2, TimeUnit.SECONDS);
    }
}
