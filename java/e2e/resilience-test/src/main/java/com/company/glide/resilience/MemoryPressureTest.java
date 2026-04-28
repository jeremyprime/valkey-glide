/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package com.company.glide.resilience;

import glide.api.GlideClusterClient;
import glide.api.models.configuration.AdvancedGlideClusterClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.PeriodicChecksManualInterval;
import glide.api.models.configuration.ReadFrom;
import io.lettuce.core.cluster.RedisClusterClient;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * E2E test: memory pressure causes client deadlock.
 *
 * <p>Sustained high-throughput load in a memory-constrained container (1400MB cgroup)
 * starves the Tokio runtime. ForkJoinPool.managedBlock() amplifies the problem by
 * spawning compensation threads when futures stop completing.
 * Based on https://github.com/alexey-temnikov/valkey-glide-thread-hang-repro
 *
 * <p><b>Expected behavior with fix (#5581 + #5752 + #5606):</b> timeouts fire via
 * the watchdog thread, errors flow back to Java, thread pool stays bounded, and
 * throughput is maintained. Without fix: futures never complete, thread pool grows
 * unboundedly, queue grows to millions, throughput drops to near-zero permanently.
 *
 * <p><b>Verification:</b> Run for 5 minutes at 500 req/s. If throughput never
 * collapses (queue stays bounded), the test passes. If stuck for > 2 minutes
 * continuously with queue > 500k, the test fails.
 */
public class MemoryPressureTest {

    private static final AtomicLong successCount = new AtomicLong();
    private static final AtomicLong errorCount = new AtomicLong();
    private static final AtomicInteger blocking = new AtomicInteger();
    private static final int KEY_COUNT = 1000;
    private static final int RPS = Integer.parseInt(
            System.getenv().getOrDefault("RPS", "500"));
    private static final int MAX_FANOUT = 16;
    private static final int TEST_DURATION_SEC = Integer.parseInt(
            System.getenv().getOrDefault("TEST_DURATION", "300")); // 5 minutes
    private static final int MAX_STUCK_SEC = 120; // fail if stuck > 2 min

    public static void main(String[] args) {
        try {
            runTest();
        } catch (Exception e) {
            System.err.println("Memory Pressure TEST FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runTest() throws Exception {
        String host = System.getenv().getOrDefault("VALKEY_HOST", "172.30.0.10");
        int port = Integer.parseInt(System.getenv().getOrDefault("VALKEY_PORT", "6379"));

        System.out.printf("Memory: max=%dMB, direct=%s%n",
                Runtime.getRuntime().maxMemory() / 1024 / 1024,
                System.getProperty("sun.nio.MaxDirectMemorySize", "default"));
        System.out.printf("Config: RPS=%d, duration=%ds, maxStuck=%ds%n",
                RPS, TEST_DURATION_SEC, MAX_STUCK_SEC);

        System.out.println("Connecting to " + host + ":" + port);
        GlideClusterClient client = createClientWithRetry(host, port);
        System.out.println("Connected.");

        // Idle Lettuce client holds direct-memory buffers, squeezing off-heap budget.
        try {
            RedisClusterClient lettuceIdle = RedisClusterClient.create("redis://" + host + ":" + port);
            lettuceIdle.connect().sync().ping();
            System.out.println("Idle Lettuce client connected (holding direct memory buffers).");
        } catch (Exception e) {
            System.out.println("Lettuce connection failed (non-critical): " + e.getMessage());
        }

        System.out.println("Pre-populating keys...");
        for (int i = 0; i < KEY_COUNT; i++) {
            for (int r = 0; r < 30; r++) {
                try { client.set("k:" + i, "v").get(5, TimeUnit.SECONDS); break; }
                catch (Exception e) { if (r == 29) throw e; Thread.sleep(1000); }
            }
        }
        System.out.println("Ready. Starting sustained load at " + RPS + " req/s...");

        // ForkJoinPool with managedBlock (matches Akka/Play pattern)
        AtomicInteger tid = new AtomicInteger();
        ForkJoinPool pool = new ForkJoinPool(50,
                p -> {
                    ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                    t.setName("worker-" + tid.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }, null, true);

        // Producer — sustained high load
        Thread producer = new Thread(() -> {
            Random rng = new Random();
            long nsPerRequest = 1_000_000_000L / RPS;
            long next = System.nanoTime();
            while (!Thread.currentThread().isInterrupted()) {
                next += nsPerRequest;
                int fanout = 1 + rng.nextInt(MAX_FANOUT);
                for (int i = 0; i < fanout; i++) {
                    int k = rng.nextInt(KEY_COUNT);
                    pool.execute(() -> doGet(client, "k:" + k));
                }
                long sleep = next - System.nanoTime();
                if (sleep > 0) LockSupport.parkNanos(sleep);
            }
        }, "producer");
        producer.setDaemon(true);
        producer.start();

        // Monitor
        long startTime = System.currentTimeMillis();
        long lastSuccess = 0;
        int stuckSeconds = 0;
        int maxStuckSeconds = 0;
        boolean everStuck = false;
        boolean recovered = false;

        while ((System.currentTimeMillis() - startTime) / 1000 < TEST_DURATION_SEC) {
            Thread.sleep(5000);
            long g = successCount.get();
            long dg = g - lastSuccess;
            lastSuccess = g;
            int blk = blocking.get();
            int poolSize = pool.getPoolSize();
            long queued = pool.getQueuedSubmissionCount();
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            int threads = Thread.activeCount();

            // Stuck = throughput collapsed (< 100/5s) after warmup, with growing queue
            // OR queue growing unboundedly (> 500k) indicating commands aren't draining
            boolean isStuck = elapsed > 30 && (
                (dg < 100 && queued > 100000) || queued > 500000);
            String tag = isStuck ? "[STUCK]" : "[MONITOR]";

            System.out.printf("%s %ds gets=%d(+%d/5s) errs=%d blocking=%d pool=%d queued=%d threads=%d%n",
                    tag, elapsed, g, dg, errorCount.get(), blk, poolSize, queued, threads);

            if (isStuck) {
                stuckSeconds += 5;
                maxStuckSeconds = Math.max(maxStuckSeconds, stuckSeconds);
                if (stuckSeconds >= 15) everStuck = true;
            } else {
                if (everStuck && stuckSeconds > 0) recovered = true;
                stuckSeconds = 0;
            }

            if (stuckSeconds >= MAX_STUCK_SEC) {
                producer.interrupt();
                pool.shutdownNow();
                System.out.printf("Memory Pressure TEST FAILED: stuck for %ds (limit %ds), pool=%d threads=%d%n",
                        stuckSeconds, MAX_STUCK_SEC, poolSize, threads);
                System.exit(1);
            }
        }

        producer.interrupt();
        pool.shutdownNow();

        System.out.println();
        System.out.printf("Results: total=%d, errors=%d, everStuck=%s, recovered=%s, maxStuck=%ds%n",
                successCount.get(), errorCount.get(), everStuck, recovered, maxStuckSeconds);

        if (!everStuck) {
            System.out.println("Memory Pressure TEST PASSED (never stuck)");
        } else if (recovered) {
            System.out.println("Memory Pressure TEST PASSED (stuck briefly, recovered)");
        } else {
            System.out.println("Memory Pressure TEST FAILED (stuck and did not recover)");
            System.exit(1);
        }

        client.close();
    }

    private static void doGet(GlideClusterClient client, String key) {
        try {
            ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
                boolean done = false;
                public boolean block() {
                    blocking.incrementAndGet();
                    try {
                        client.get(key).get();
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                    } finally {
                        blocking.decrementAndGet();
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
                                .requestTimeout(250)
                                .readFrom(ReadFrom.AZ_AFFINITY_REPLICAS_AND_PRIMARY)
                                .advancedConfiguration(
                                        AdvancedGlideClusterClientConfiguration.builder()
                                                .refreshTopologyFromInitialNodes(true)
                                                .connectionTimeout(1000)
                                                .periodicChecks(PeriodicChecksManualInterval.builder()
                                                        .durationInSec(60).build())
                                                .build())
                                .build()
                ).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("Waiting for cluster... attempt " + (i + 1));
                Thread.sleep(2000);
            }
        }
        throw new RuntimeException("Cannot connect to cluster");
    }
}
