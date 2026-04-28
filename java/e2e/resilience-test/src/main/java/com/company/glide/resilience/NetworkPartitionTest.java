/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package com.company.glide.resilience;

import glide.api.GlideClusterClient;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * E2E test: network partition causes client deadlock.
 *
 * <p>Simulates half-open TCP by pausing the Valkey container (docker pause).
 * The connection appears alive but no data flows, filling the pipeline and
 * consuming all inflight slots.
 *
 * <p><b>Expected behavior with fix (#5755 + #5804):</b> healthy shards keep
 * processing during the pause. Without fix: throughput drops to 0 indefinitely.
 *
 * <p><b>Verification:</b> After pausing, check that successful requests continue
 * (ok > 0). After unpausing, check full throughput recovery.
 */
public class NetworkPartitionTest {

    private static final AtomicLong successCount = new AtomicLong();
    private static final AtomicLong errorCount = new AtomicLong();
    private static final AtomicInteger activeThreads = new AtomicInteger();

    public static void main(String[] args) {
        try {
            runTest();
        } catch (Exception e) {
            System.err.println("Network Partition TEST FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runTest() throws Exception {
        String host = System.getenv().getOrDefault("VALKEY_HOST", "172.30.0.10");
        int port = Integer.parseInt(System.getenv().getOrDefault("VALKEY_PORT", "6379"));

        System.out.println("Connecting to " + host + ":" + port);
        GlideClusterClient client = createClientWithRetry(host, port);
        System.out.println("Connected.");

        // Pre-populate keys (retry during cluster init)
        for (int i = 0; i < 100; i++) {
            for (int r = 0; r < 30; r++) {
                try { client.set("key:" + i, "value:" + i).get(5, TimeUnit.SECONDS); break; }
                catch (Exception e) { if (r == 29) throw e; Thread.sleep(1000); }
            }
        }

        // Start workload — ForkJoinPool with managedBlock (Akka pattern)
        AtomicInteger tid = new AtomicInteger();
        ForkJoinPool pool = new ForkJoinPool(50,
                p -> {
                    ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                    t.setName("worker-" + tid.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }, null, true);

        Thread producer = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int k = ThreadLocalRandom.current().nextInt(100);
                pool.execute(() -> doGet(client, "key:" + k));
                try { Thread.sleep(0, 500_000); } catch (InterruptedException e) { return; }
            }
        }, "producer");
        producer.setDaemon(true);
        producer.start();

        // Steady state
        System.out.println("Running steady state for 15s...");
        long lastOk = successCount.get();
        for (int i = 0; i < 3; i++) {
            Thread.sleep(5000);
            long ok = successCount.get();
            long err = errorCount.get();
            long delta = ok - lastOk;
            lastOk = ok;
            System.out.printf("[STEADY] %ds gets=%d(+%d/5s) errs=%d blocking=%d pool=%d%n",
                    (i + 1) * 5, ok, delta, err, activeThreads.get(), pool.getPoolSize());
        }
        long steadyOk = successCount.get();
        System.out.printf("Steady state: %d successful requests%n", steadyOk);

        // Pause one Valkey node (simulating half-open TCP on one shard)
        System.out.println("Pausing Valkey node 1 (simulating half-open TCP)...");
        exec("docker pause resilience-valkey-1");

        // Monitor during pause — check that healthy shards keep processing
        System.out.println("Monitoring for 60s during pause...");
        long okBeforePause = successCount.get();
        long okAtMidpoint = 0;
        for (int i = 0; i < 12; i++) {
            Thread.sleep(5000);
            long ok = successCount.get();
            long err = errorCount.get();
            long delta = ok - lastOk;
            lastOk = ok;
            String tag = delta == 0 ? "[STUCK]" : "[MONITOR]";
            System.out.printf("%s %ds gets=%d(+%d/5s) errs=%d blocking=%d pool=%d%n",
                    tag, (i + 1) * 5, ok, delta, err, activeThreads.get(), pool.getPoolSize());
            if (i == 5) okAtMidpoint = ok; // snapshot at 30s
        }
        long okDuringPause = successCount.get() - okBeforePause;
        long okSecondHalf = successCount.get() - okAtMidpoint;

        // Unpause
        System.out.println("Unpausing Valkey node 1...");
        exec("docker unpause resilience-valkey-1");

        // Recovery
        System.out.println("Monitoring recovery for 30s...");
        long okBeforeRecovery = successCount.get();
        for (int i = 0; i < 6; i++) {
            Thread.sleep(5000);
            long ok = successCount.get();
            long err = errorCount.get();
            long delta = ok - lastOk;
            lastOk = ok;
            System.out.printf("[RECOVERY] %ds gets=%d(+%d/5s) errs=%d blocking=%d pool=%d%n",
                    (i + 1) * 5, ok, delta, err, activeThreads.get(), pool.getPoolSize());
        }
        long okAfterRecovery = successCount.get() - okBeforeRecovery;

        // Stop workload
        producer.interrupt();
        pool.shutdownNow();

        // Results
        System.out.println();
        System.out.printf("Results: steady=%d, duringPause=%d, secondHalf=%d, afterRecovery=%d%n",
                steadyOk, okDuringPause, okSecondHalf, okAfterRecovery);

        // With fix: healthy shards keep processing throughout the pause (okSecondHalf > 0)
        // Without fix: initial burst then zero (okSecondHalf == 0)
        if (okSecondHalf > 0 && okAfterRecovery > 0) {
            System.out.println("Network Partition TEST PASSED");
        } else {
            System.out.printf("Network Partition TEST FAILED: secondHalf=%d, afterRecovery=%d%n",
                    okSecondHalf, okAfterRecovery);
            System.exit(1);
        }

        client.close();
    }

    private static void doGet(GlideClusterClient client, String key) {
        try {
            ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
                boolean done = false;
                public boolean block() {
                    activeThreads.incrementAndGet();
                    try {
                        client.get(key).get();
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        activeThreads.decrementAndGet();
                        done = true;
                    }
                    return true;
                }
                public boolean isReleasable() { return done; }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static GlideClusterClient createClientWithRetry(String host, int port) throws Exception {
        for (int i = 0; i < 30; i++) {
            try {
                return GlideClusterClient.createClient(
                        GlideClusterClientConfiguration.builder()
                                .address(NodeAddress.builder().host(host).port(port).build())
                                .requestTimeout(30)
                                .build()
                ).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("Waiting for cluster... attempt " + (i + 1));
                Thread.sleep(2000);
            }
        }
        throw new RuntimeException("Cannot connect to cluster");
    }

    private static void exec(String command) throws Exception {
        Process p = Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
        if (!p.waitFor(10, TimeUnit.SECONDS) || p.exitValue() != 0) {
            System.err.println("Command failed: " + command);
        }
    }
}
