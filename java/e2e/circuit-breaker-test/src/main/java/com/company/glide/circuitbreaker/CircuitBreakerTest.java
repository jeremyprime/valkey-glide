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
                                                                        .windowSize(15000)
                                                                        .errorThreshold(3)
                                                                        .openTimeout(5000)
                                                                        .countTimeouts(true)
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

        // Phase 2: Pause one node (freezes process, half-open TCP — no RST sent)
        // With replicas, the cluster stays up but the paused node's connection times out
        System.out.println("[PHASE 2] Pausing cb-node-1 (docker pause)...");
        executeCommand("docker pause cb-node-1");
        Thread.sleep(2000); // Let the connection detect the pause

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

        // Verify: breaker should have tripped — evidence is that we got only a few
        // timeouts (3-4 to trip the breaker) and then recovered quickly.
        // Without the breaker, we'd get ~66 timeouts (200 commands × 1/3 to paused node × 3s each).
        // With the breaker, we get 3-4 timeouts then fast recovery.
        if (timeoutErrors.get() == 0 && errs.get() == 0) {
            throw new RuntimeException(
                    "Phase 3 failed: no errors at all — node pause may not have worked");
        }
        if (elapsed > 60000) {
            throw new RuntimeException(
                    "Phase 3 failed: took "
                            + elapsed
                            + "ms — breaker likely didn't trip (expected <30s with tripping)");
        }
        System.out.printf(
                "[PHASE 3] Breaker tripped after %d timeouts, recovered in %dms%n",
                timeoutErrors.get(), elapsed);

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

        // Phase 5: Verify recovery from timeout path
        System.out.println("[PHASE 5] Verifying recovery from timeout...");
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
        if (ops.get() < 250) {
            throw new RuntimeException(
                    "Phase 5 failed: only " + ops.get() + "/300 ops succeeded after timeout recovery.");
        }

        // Phase 6: Connection error path (iptables block)
        // This tests transport errors (FatalSendError) which trip the breaker
        // without needing countTimeouts=true
        System.out.println("[PHASE 6] Blocking traffic to node-2 via iptables...");
        executeCommand("iptables -A OUTPUT -d " + NODE_2 + " -j DROP");
        Thread.sleep(1000);

        System.out.println(
                "[PHASE 6] Sending commands (expecting breaker to trip on connection errors)...");
        ops.set(0);
        errs.set(0);
        long connErrors = 0;
        start = System.currentTimeMillis();
        for (int i = 0; i < 200; i++) {
            try {
                client.get("cb:" + (i % 100)).get();
                ops.incrementAndGet();
            } catch (Exception e) {
                errs.incrementAndGet();
            }
        }
        elapsed = System.currentTimeMillis() - start;
        System.out.printf("[PHASE 6] %d ops, %d errors in %dms%n", ops.get(), errs.get(), elapsed);

        // With breaker: connection errors trip fast, healthy nodes continue
        // Without breaker: each command to blocked node waits for pipeline timeout (100ms)
        //   then retries, eventually timing out at 3s
        if (errs.get() == 0) {
            throw new RuntimeException("Phase 6 failed: no errors — iptables may not have worked");
        }
        if (elapsed > 60000) {
            throw new RuntimeException(
                    "Phase 6 failed: took " + elapsed + "ms — breaker didn't trip on connection errors");
        }
        System.out.printf(
                "[PHASE 6] Connection error path: %d errors, recovered in %dms%n", errs.get(), elapsed);

        // Phase 7: Remove iptables block and verify recovery
        System.out.println("[PHASE 7] Removing iptables block...");
        executeCommand("iptables -D OUTPUT -d " + NODE_2 + " -j DROP");
        Thread.sleep(7000); // Wait for breaker open_timeout + probe

        System.out.println("[PHASE 7] Verifying recovery from connection errors...");
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
        System.out.printf("[PHASE 7] %d ops, %d errors%n", ops.get(), errs.get());
        if (ops.get() < 250) {
            throw new RuntimeException(
                    "Phase 7 failed: only " + ops.get() + "/300 ops after connection error recovery.");
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
