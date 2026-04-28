/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package com.company.glide.resilience;

import glide.api.GlideClusterClient;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Circuit Breaker Benchmark — measures specific metrics during a "sick node" scenario
 * to quantify the CB's effect vs without CB.
 *
 * The sick node scenario: one node periodically sleeps (DEBUG SLEEP 150ms every 1s).
 * With requestTimeout=100ms, commands during sleep timeout but the pipeline never fills.
 * This simulates gradual degradation (Event 1 in #5806).
 *
 * With CB (countTimeouts=true): trips after 5 timeouts, isolates sick node, healthy nodes continue.
 * Without CB: client keeps routing to sick node for the full test, accumulating timeouts.
 *
 * Run on CB branch and main branch, compare numbers.
 * On main branch, set ENABLE_CB=false (or leave as-is, it will fail to load CB classes gracefully).
 */
public class CircuitBreakerBenchmark {

    private static final AtomicLong successCount = new AtomicLong();
    private static final AtomicLong errorCount = new AtomicLong();
    private static final AtomicLong totalErrorLatencyNs = new AtomicLong();
    private static final AtomicLong fastErrors = new AtomicLong(); // < 10ms
    private static final AtomicLong slowErrors = new AtomicLong(); // >= 10ms
    private static final AtomicInteger activeThreads = new AtomicInteger();
    private static volatile int peakPool = 0;
    private static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "172.30.0.11";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6379;

        GlideClusterClient client = createClient(host, port);
        System.out.println("[BENCH] Connected. Pre-populating...");
        for (int i = 0; i < 100; i++) {
            for (int attempt = 0; attempt < 30; attempt++) {
                try { client.set("bench:" + i, "v" + i).get(); break; }
                catch (Exception e) { if (attempt == 29) throw e; Thread.sleep(500); }
            }
        }

        // ForkJoinPool with managedBlock (Akka/Play pattern)
        AtomicInteger tid = new AtomicInteger();
        ForkJoinPool pool = new ForkJoinPool(500,
                p -> {
                    ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                    t.setName("bench-" + tid.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }, null, true);

        // Producer thread
        Thread producer = new Thread(() -> {
            while (running) {
                int k = ThreadLocalRandom.current().nextInt(100);
                pool.execute(() -> doGet(client, "bench:" + k));
                try { Thread.sleep(0, 100_000); } catch (InterruptedException e) { return; }
            }
        }, "producer");
        producer.setDaemon(true);
        producer.start();

        // Warm up 10s
        System.out.println("[BENCH] Warming up 10s...");
        Thread.sleep(10_000);

        // Reset counters
        successCount.set(0);
        errorCount.set(0);
        totalErrorLatencyNs.set(0);
        fastErrors.set(0);
        slowErrors.set(0);
        peakPool = pool.getPoolSize();

        // Make node 1 "sick" — DEBUG SLEEP 150ms every 1s (alive but intermittently slow)
        // With requestTimeout=100ms, commands during sleep timeout but pipeline never fills.
        // Only the CB detects this sustained timeout pattern.
        System.out.println("[BENCH] Making node 1 sick (DEBUG SLEEP 150ms every 1s, timeout=100ms)...");
        long pauseStart = System.currentTimeMillis();
        Thread sickNode = new Thread(() -> {
            while (running) {
                try {
                    exec("docker exec resilience-valkey-1 valkey-cli DEBUG SLEEP 0.15");
                    Thread.sleep(1000);
                } catch (Exception e) { return; }
            }
        }, "sick-node");
        sickNode.setDaemon(true);
        sickNode.start();

        // Monitor for 30s during degradation
        for (int i = 0; i < 6; i++) {
            Thread.sleep(5000);
            int currentPool = pool.getPoolSize();
            if (currentPool > peakPool) peakPool = currentPool;
            long elapsed = (System.currentTimeMillis() - pauseStart) / 1000;
            System.out.printf("[BENCH] t=%ds ops=%d errs=%d fast_errs=%d slow_errs=%d pool=%d%n",
                    elapsed, successCount.get(), errorCount.get(),
                    fastErrors.get(), slowErrors.get(), currentPool);
        }

        // Stop the sick node simulation
        System.out.println("[BENCH] Stopping sick node simulation...");
        running = false;
        sickNode.interrupt();
        Thread.sleep(5000);

        producer.interrupt();
        pool.shutdownNow();

        // Results
        long ops = successCount.get();
        long errs = errorCount.get();
        long fast = fastErrors.get();
        long slow = slowErrors.get();
        double avgErrLatencyMs = errs > 0 ? (totalErrorLatencyNs.get() / errs) / 1_000_000.0 : 0;

        System.out.println("\n========== CIRCUIT BREAKER BENCHMARK RESULTS ==========");
        System.out.printf("Successful ops during 30s pause:  %d (%.0f ops/s)%n", ops, ops / 30.0);
        System.out.printf("Total errors during pause:        %d%n", errs);
        System.out.printf("Fast errors (<10ms):              %d (%.1f%%)%n", fast, errs > 0 ? fast * 100.0 / errs : 0);
        System.out.printf("Slow errors (>=10ms, timeouts):   %d (%.1f%%)%n", slow, errs > 0 ? slow * 100.0 / errs : 0);
        System.out.printf("Avg error latency:                %.1f ms%n", avgErrLatencyMs);
        System.out.printf("Peak thread pool size:            %d%n", peakPool);
        System.out.println("=======================================================");

        client.close();
    }

    private static void doGet(GlideClusterClient client, String key) {
        activeThreads.incrementAndGet();
        long start = System.nanoTime();
        try {
            ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
                volatile boolean done = false;
                @Override public boolean block() throws InterruptedException {
                    try { client.get(key).get(); }
                    catch (Exception e) { throw new RuntimeException(e); }
                    done = true;
                    return true;
                }
                @Override public boolean isReleasable() { return done; }
            });
            successCount.incrementAndGet();
        } catch (Exception e) {
            long latencyNs = System.nanoTime() - start;
            errorCount.incrementAndGet();
            totalErrorLatencyNs.addAndGet(latencyNs);
            if (latencyNs < 10_000_000L) { // < 10ms
                fastErrors.incrementAndGet();
            } else {
                slowErrors.incrementAndGet();
            }
        } finally {
            activeThreads.decrementAndGet();
        }
    }

    private static GlideClusterClient createClient(String host, int port) throws Exception {
        for (int i = 0; i < 30; i++) {
            try {
                var builder = GlideClusterClientConfiguration.builder()
                        .address(NodeAddress.builder().host(host).port(port).build())
                        .requestTimeout(100)
                        .inflightRequestsLimit(1000);
                // Add CB config via reflection (works on CB branch, skipped on main)
                try {
                    Object cbBuilder = Class.forName("glide.api.models.configuration.CircuitBreakerConfiguration")
                            .getMethod("builder").invoke(null);
                    cbBuilder = cbBuilder.getClass().getMethod("errorThreshold", int.class).invoke(cbBuilder, 5);
                    cbBuilder = cbBuilder.getClass().getMethod("windowSize", int.class).invoke(cbBuilder, 10000);
                    cbBuilder = cbBuilder.getClass().getMethod("openTimeout", int.class).invoke(cbBuilder, 5000);
                    cbBuilder = cbBuilder.getClass().getMethod("countTimeouts", boolean.class).invoke(cbBuilder, true);
                    Object cbConfig = cbBuilder.getClass().getMethod("build").invoke(cbBuilder);

                    Object advBuilder = Class.forName("glide.api.models.configuration.AdvancedGlideClusterClientConfiguration")
                            .getMethod("builder").invoke(null);
                    advBuilder.getClass().getMethod("circuitBreakerConfiguration",
                            Class.forName("glide.api.models.configuration.CircuitBreakerConfiguration"))
                            .invoke(advBuilder, cbConfig);
                    Object advConfig = advBuilder.getClass().getMethod("build").invoke(advBuilder);

                    builder.getClass().getMethod("advancedConfiguration",
                            Class.forName("glide.api.models.configuration.AdvancedGlideClusterClientConfiguration"))
                            .invoke(builder, advConfig);
                    System.out.println("[BENCH] Circuit breaker ENABLED (countTimeouts=true, threshold=5)");
                } catch (ClassNotFoundException e) {
                    System.out.println("[BENCH] Circuit breaker NOT available (main branch)");
                } catch (Exception e) {
                    System.out.println("[BENCH] CB config failed (running without CB): " + e.getMessage());
                }
                return GlideClusterClient.createClient(
                        (GlideClusterClientConfiguration) builder.getClass().getMethod("build").invoke(builder)
                ).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("[BENCH] Connect attempt " + (i+1) + ": " + e.getMessage());
                Thread.sleep(2000);
            }
        }
        throw new RuntimeException("Cannot connect");
    }

    private static void exec(String cmd) throws Exception {
        Runtime.getRuntime().exec(new String[]{"sh", "-c", cmd}).waitFor();
    }
}
