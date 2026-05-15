/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package com.company.glide.circuitbreaker;

import glide.api.GlideClusterClient;
import glide.api.models.configuration.AdvancedGlideClusterClientConfiguration;
import glide.api.models.configuration.CircuitBreakerConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 * E2E test for the per-node circuit breaker.
 *
 * <p>Test flow:
 *
 * <ol>
 *   <li>Connect to a 3-node cluster with circuit breaker enabled (low threshold for fast tripping)
 *   <li>Verify normal operation (all nodes healthy)
 *   <li>Pause one node via docker pause (simulates dead node / half-open TCP)
 *   <li>Send commands — verify breaker trips (fast errors instead of timeouts)
 *   <li>Verify healthy nodes continue serving at full throughput
 *   <li>Unpause the node
 *   <li>Verify breaker closes and node resumes serving
 * </ol>
 */
public class CircuitBreakerTest {

    private static final String NODE_1 = "172.32.0.11";
    private static final String NODE_2 = "172.32.0.12";
    private static final String NODE_3 = "172.32.0.13";
    private static final int PORT = 6379;
    private static final int CLUSTER_INIT_WAIT_MS = 8000;

    public static void main(String[] args) {
        try {
            runTest();
            System.out.println("Circuit Breaker Test PASSED");
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Circuit Breaker Test FAILED");
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runTest() throws Exception {
        System.out.println("[INIT] Waiting for cluster...");
        Thread.sleep(CLUSTER_INIT_WAIT_MS);

        System.out.println("[INIT] Connecting with circuit breaker enabled...");
        GlideClusterClient client =
                GlideClusterClient.createClient(
                                GlideClusterClientConfiguration.builder()
                                        .address(NodeAddress.builder().host(NODE_1).port(PORT).build())
                                        .requestTimeout(3000)
                                        .advancedConfiguration(
                                                AdvancedGlideClusterClientConfiguration.builder()
                                                        .connectionTimeout(3000)
                                                        .circuitBreakerConfiguration(
                                                                CircuitBreakerConfiguration.builder()
                                                                        .windowSize(5000)
                                                                        .errorThreshold(3)
                                                                        .openTimeout(5000)
                                                                        .build())
                                                        .build())
                                        .build())
                        .get();

        System.out.println("[INIT] Connected. Pre-populating keys...");
        for (int i = 0; i < 100; i++) {
            client.set("cb:" + i, "value-" + i).get();
        }

        // Phase 1: Verify normal operation
        System.out.println("[PHASE 1] Verifying normal operation...");
        AtomicLong ops = new AtomicLong();
        AtomicLong errs = new AtomicLong();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            try {
                client.get("cb:" + (i % 100)).get();
                ops.incrementAndGet();
            } catch (Exception e) {
                errs.incrementAndGet();
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("[PHASE 1] %d ops, %d errors in %dms%n", ops.get(), errs.get(), elapsed);
        if (errs.get() > 0) {
            throw new RuntimeException("Phase 1 failed: errors during normal operation");
        }

        // Phase 2: Pause one node
        System.out.println("[PHASE 2] Pausing cb-node-1 (docker pause)...");
        executeCommand("docker pause cb-node-1");
        Thread.sleep(1000); // Let the pause take effect

        // Phase 3: Send commands — some will timeout, breaker should trip
        System.out.println("[PHASE 3] Sending commands (expecting breaker to trip)...");
        ops.set(0);
        errs.set(0);
        AtomicLong circuitBreakerErrors = new AtomicLong();
        AtomicLong timeoutErrors = new AtomicLong();
        start = System.currentTimeMillis();

        for (int i = 0; i < 200; i++) {
            try {
                client.get("cb:" + (i % 100)).get();
                ops.incrementAndGet();
            } catch (Exception e) {
                errs.incrementAndGet();
                String msg = e.getMessage();
                if (msg != null && msg.contains("Circuit breaker")) {
                    circuitBreakerErrors.incrementAndGet();
                } else if (msg != null && (msg.contains("timed out") || msg.contains("Timeout"))) {
                    timeoutErrors.incrementAndGet();
                }
            }
        }
        elapsed = System.currentTimeMillis() - start;
        System.out.printf(
                "[PHASE 3] %d ops, %d errors (%d circuit breaker, %d timeout) in %dms%n",
                ops.get(), errs.get(), circuitBreakerErrors.get(), timeoutErrors.get(), elapsed);

        // Verify: breaker should have tripped (circuit breaker errors > 0)
        if (circuitBreakerErrors.get() == 0) {
            throw new RuntimeException(
                    "Phase 3 failed: circuit breaker never tripped. "
                            + "Expected fast rejection but got "
                            + timeoutErrors.get()
                            + " timeouts instead.");
        }
        System.out.printf(
                "[PHASE 3] Circuit breaker tripped successfully (%d rejections)%n",
                circuitBreakerErrors.get());

        // Verify: healthy nodes still served some requests
        if (ops.get() == 0) {
            throw new RuntimeException(
                    "Phase 3 failed: zero successful ops. Healthy nodes should still serve.");
        }
        System.out.printf("[PHASE 3] Healthy nodes served %d requests%n", ops.get());

        // Phase 4: Unpause and verify recovery
        System.out.println("[PHASE 4] Unpausing cb-node-1...");
        executeCommand("docker unpause cb-node-1");

        // Wait for breaker open_timeout (5s) + probe
        System.out.println("[PHASE 4] Waiting for breaker to close (open_timeout=5s)...");
        Thread.sleep(7000);

        // Phase 5: Verify full recovery
        System.out.println("[PHASE 5] Verifying recovery...");
        ops.set(0);
        errs.set(0);
        for (int i = 0; i < 300; i++) {
            try {
                client.get("cb:" + (i % 100)).get();
                ops.incrementAndGet();
            } catch (Exception e) {
                errs.incrementAndGet();
            }
        }
        System.out.printf("[PHASE 5] %d ops, %d errors%n", ops.get(), errs.get());

        // Allow some errors during recovery transition but most should succeed
        if (ops.get() < 250) {
            throw new RuntimeException(
                    "Phase 5 failed: only "
                            + ops.get()
                            + "/300 ops succeeded after recovery. "
                            + "Breaker may not have closed.");
        }

        client.close();
        System.out.println("[DONE] All phases passed.");
    }

    private static void executeCommand(String command) throws Exception {
        Process process = Runtime.getRuntime().exec(new String[] {"sh", "-c", command});
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            System.err.println("Command failed: " + command + " (exit " + exitCode + ")");
        }
    }
}
