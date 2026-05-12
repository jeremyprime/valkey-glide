/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.configuration;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Configuration for the per-node circuit breaker in cluster mode.
 *
 * <p>The circuit breaker detects unresponsive nodes and stops routing commands to them, preserving
 * throughput to healthy nodes. When a node accumulates connection-level errors beyond the
 * threshold, the breaker trips: drops the connection (releasing stuck in-flight commands), rejects
 * new commands immediately, and probes for recovery with exponential backoff.
 *
 * @example
 *     <pre>{@code
 * CircuitBreakerConfiguration.builder()
 *     .windowSize(10000)
 *     .errorThreshold(10)
 *     .openTimeout(5000)
 *     .countTimeouts(false)
 *     .build();
 * }</pre>
 */
@Getter
@Builder
@ToString
public class CircuitBreakerConfiguration {

    /** Sliding window duration in milliseconds for error counting. Default: 10000 (10s). */
    @Builder.Default private final int windowSize = 10000;

    /** Number of connection-level errors within the window to trip the breaker. Default: 10. */
    @Builder.Default private final int errorThreshold = 10;

    /** Time in milliseconds in Open state before allowing a probe request. Default: 5000 (5s). */
    @Builder.Default private final int openTimeout = 5000;

    /**
     * When true, command timeouts count toward tripping the breaker. Default: false.
     *
     * <p>Enable this if your nodes have reliable latency and timeouts indicate node failure. Leave
     * disabled if timeouts may be caused by client-side pressure (e.g., GC pauses).
     */
    @Builder.Default private final boolean countTimeouts = false;
}
