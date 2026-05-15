//! Per-node circuit breaker for cluster connections.
//!
//! Detects unresponsive nodes via a sliding window of transport errors and
//! rejects commands to those nodes immediately, preserving throughput to
//! healthy nodes. When tripped, the caller drops the dead connection to
//! release stuck in-flight commands.
//!
//! State machine: Closed → Open → HalfOpen → Closed (on probe success)
//!                                HalfOpen → Open  (on probe failure)

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::ErrorKind;
use crate::RedisError;
use logger_core::{log_info_lazy, log_warn_lazy};

/// Circuit breaker phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — errors tracked in sliding window.
    Closed,
    /// Node unhealthy — commands rejected immediately.
    Open,
    /// One probe allowed to test recovery.
    HalfOpen,
}

/// Circuit breaker configuration.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Sliding window for error counting.
    pub window_size: Duration,
    /// Errors within window to trip the breaker.
    pub error_threshold: u32,
    /// Time in Open state before allowing a probe.
    pub open_timeout: Duration,
    /// When true, timeouts count toward tripping. Default: false.
    pub count_timeouts: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            window_size: Duration::from_millis(10_000),
            error_threshold: 10,
            open_timeout: Duration::from_millis(5_000),
            count_timeouts: false,
        }
    }
}

/// Per-node circuit breaker.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: RwLock<BreakerState>,
    address: String,
    /// Number of times the breaker has tripped.
    trip_count: AtomicU64,
}

struct BreakerState {
    phase: CircuitState,
    errors: VecDeque<Instant>,
    opened_at: Option<Instant>,
    probe_in_flight: bool,
    /// Number of consecutive trips without a successful close.
    /// Used for exponential backoff on open_timeout.
    consecutive_trips: u32,
}

impl CircuitBreaker {
    /// Create a new circuit breaker for the given node address.
    pub fn new(config: CircuitBreakerConfig, address: String) -> Self {
        Self {
            config,
            state: RwLock::new(BreakerState {
                phase: CircuitState::Closed,
                errors: VecDeque::new(),
                opened_at: None,
                probe_in_flight: false,
                consecutive_trips: 0,
            }),
            address,
            trip_count: AtomicU64::new(0),
        }
    }

    /// Check if a command should be allowed through.
    /// Returns `Ok(true)` for a probe, `Ok(false)` for normal, `Err` if rejected.
    pub fn on_entry(&self) -> Result<bool, RedisError> {
        // Fast path: read lock for the common Closed state
        {
            let state = self.state.read().unwrap_or_else(|e| e.into_inner());
            if state.phase == CircuitState::Closed {
                return Ok(false);
            }
        }
        // Slow path: write lock for Open/HalfOpen state transitions
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();

        match state.phase {
            CircuitState::Closed => Ok(false),
            CircuitState::Open => {
                let opened_at = state.opened_at.unwrap_or(now);
                let elapsed = now.duration_since(opened_at);
                // Exponential backoff: open_timeout * 2^(consecutive_trips - 1), max 16x
                // First trip uses the configured open_timeout (2^0 = 1x)
                let backoff_multiplier = 1u32 << state.consecutive_trips.saturating_sub(1).min(4);
                let effective_timeout = self.config.open_timeout * backoff_multiplier;
                if elapsed >= effective_timeout {
                    state.phase = CircuitState::HalfOpen;
                    state.probe_in_flight = true;
                    Ok(true)
                } else {
                    Err(RedisError::from((
                        ErrorKind::ClientError,
                        "Circuit breaker open",
                        self.address.clone(),
                    )))
                }
            }
            CircuitState::HalfOpen => {
                if !state.probe_in_flight {
                    state.probe_in_flight = true;
                    Ok(true)
                } else {
                    Err(RedisError::from((
                        ErrorKind::ClientError,
                        "Circuit breaker half-open, probe in flight",
                        self.address.clone(),
                    )))
                }
            }
        }
    }

    /// Report command result. Returns `true` if the breaker just tripped.
    pub fn on_result(&self, is_probe: bool, err: Option<&RedisError>) -> bool {
        let is_transport_error = err.is_some_and(|e| self.is_transport_error(e));
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();

        match state.phase {
            CircuitState::Closed => {
                if is_transport_error {
                    let window = self.config.window_size;
                    while state
                        .errors
                        .front()
                        .is_some_and(|t| now.duration_since(*t) > window)
                    {
                        state.errors.pop_front();
                    }
                    state.errors.push_back(now);

                    if state.errors.len() as u32 >= self.config.error_threshold {
                        state.phase = CircuitState::Open;
                        state.opened_at = Some(now);
                        state.errors.clear();
                        state.consecutive_trips = state.consecutive_trips.saturating_add(1);
                        self.trip_count.fetch_add(1, Ordering::Relaxed);
                        log_warn_lazy!(
                            "circuit_breaker",
                            format!(
                                "Circuit breaker tripped for node {}. Rejecting commands for {:?} (trip #{})",
                                self.address, self.config.open_timeout * (1u32 << state.consecutive_trips.saturating_sub(1).min(4)),
                                state.consecutive_trips
                            )
                        );
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => {
                if is_probe {
                    state.probe_in_flight = false;
                    if err.is_some() {
                        // Any error (transport or otherwise) means the node is still unhealthy
                        state.phase = CircuitState::Open;
                        state.opened_at = Some(now);
                        state.consecutive_trips = state.consecutive_trips.saturating_add(1);
                        self.trip_count.fetch_add(1, Ordering::Relaxed);
                        log_warn_lazy!(
                            "circuit_breaker",
                            format!(
                                "Probe failed for node {}. Breaker remains open (trip #{})",
                                self.address, state.consecutive_trips
                            )
                        );
                    } else {
                        // Probe succeeded (no error) — node is healthy again
                        state.phase = CircuitState::Closed;
                        state.opened_at = None;
                        state.consecutive_trips = 0;
                        log_info_lazy!(
                            "circuit_breaker",
                            format!("Probe succeeded for node {}. Breaker closed", self.address)
                        );
                    }
                }
                false
            }
            CircuitState::Open => false,
        }
    }

    /// Returns true for transport-level errors that count toward tripping.
    pub fn is_transport_error(&self, err: &RedisError) -> bool {
        // Exclude client-side timeouts unless explicitly configured to count them.
        // Under Tokio starvation, timeouts fire on all nodes simultaneously (client issue),
        // but connection drops only happen when the node is actually unreachable.
        if err.is_timeout() {
            return self.config.count_timeouts;
        }
        matches!(
            err.kind(),
            ErrorKind::IoError
                | ErrorKind::FatalSendError
                | ErrorKind::FatalReceiveError
                | ErrorKind::ProtocolDesync
        ) || err.is_connection_dropped()
    }

    /// Current breaker state.
    pub fn state(&self) -> CircuitState {
        self.state.read().unwrap_or_else(|e| e.into_inner()).phase
    }

    /// Returns true if the breaker is currently blocking commands (Open and timeout not elapsed).
    pub fn is_open(&self) -> bool {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        match state.phase {
            CircuitState::Open => {
                let now = Instant::now();
                let opened_at = state.opened_at.unwrap_or(now);
                let backoff_multiplier = 1u32 << state.consecutive_trips.saturating_sub(1).min(4);
                let effective_timeout = self.config.open_timeout * backoff_multiplier;
                now.duration_since(opened_at) < effective_timeout
            }
            _ => false,
        }
    }

    /// Node address this breaker is for.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Number of times this breaker has tripped.
    pub fn trip_count(&self) -> u64 {
        self.trip_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transport_err() -> RedisError {
        RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset))
    }

    fn timeout_err() -> RedisError {
        RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut))
    }

    fn server_err() -> RedisError {
        RedisError::from((ErrorKind::ResponseError, "WRONGTYPE"))
    }

    #[test]
    fn closed_allows_commands() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default(), "node:6379".into());
        assert!(!cb.on_entry().unwrap());
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn trips_after_threshold() {
        let config = CircuitBreakerConfig {
            error_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());
        let err = transport_err();

        assert!(!cb.on_result(false, Some(&err)));
        assert!(!cb.on_result(false, Some(&err)));
        assert_eq!(cb.state(), CircuitState::Closed);

        assert!(cb.on_result(false, Some(&err)));
        assert_eq!(cb.state(), CircuitState::Open);
        assert_eq!(cb.trip_count(), 1);
    }

    #[test]
    fn server_errors_dont_trip() {
        let config = CircuitBreakerConfig {
            error_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());
        let err = server_err();

        for _ in 0..10 {
            cb.on_result(false, Some(&err));
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn open_rejects_commands() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            open_timeout: Duration::from_millis(60_000),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());

        assert!(cb.on_result(false, Some(&transport_err())));
        assert!(cb.on_entry().is_err());
    }

    #[test]
    fn half_open_probe_success_closes() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            open_timeout: Duration::ZERO,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());

        assert!(cb.on_result(false, Some(&transport_err())));
        assert_eq!(cb.state(), CircuitState::Open);

        let is_probe = cb.on_entry().unwrap();
        assert!(is_probe);
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        cb.on_result(true, None);
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn half_open_probe_failure_reopens() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            open_timeout: Duration::ZERO,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());

        assert!(cb.on_result(false, Some(&transport_err())));
        let is_probe = cb.on_entry().unwrap();
        assert!(is_probe);

        cb.on_result(true, Some(&transport_err()));
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn half_open_rejects_while_probe_in_flight() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            open_timeout: Duration::ZERO,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());

        assert!(cb.on_result(false, Some(&transport_err())));
        assert!(cb.on_entry().unwrap()); // probe allowed
        assert!(cb.on_entry().is_err()); // second rejected
    }

    #[test]
    fn errors_outside_window_dont_count() {
        let config = CircuitBreakerConfig {
            window_size: Duration::ZERO, // instant expiry
            error_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());
        let err = transport_err();

        // Each error expires before the next is counted
        std::thread::sleep(std::time::Duration::from_millis(1));
        cb.on_result(false, Some(&err));
        std::thread::sleep(std::time::Duration::from_millis(1));
        cb.on_result(false, Some(&err));
        std::thread::sleep(std::time::Duration::from_millis(1));
        cb.on_result(false, Some(&err));

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn success_doesnt_affect_closed_state() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig::default(), "node:6379".into());
        assert!(!cb.on_result(false, None));
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn timeouts_dont_trip_by_default() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());
        let err = timeout_err();

        // Timeouts should not trip with count_timeouts=false (default)
        for _ in 0..10 {
            cb.on_result(false, Some(&err));
        }
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn exponential_backoff_on_consecutive_trips() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            open_timeout: Duration::from_millis(100),
            ..Default::default()
        };
        let cb = CircuitBreaker::new(config, "node:6379".into());
        let err = transport_err();

        // Trip 1 → effective timeout = 100ms * 1 = 100ms (consecutive_trips=1, shift=0)
        assert!(cb.on_result(false, Some(&err)));
        assert!(cb.on_entry().is_err()); // immediately rejected

        // Wait past first backoff
        std::thread::sleep(Duration::from_millis(250));
        assert!(cb.on_entry().unwrap()); // probe allowed

        // Probe fails → Trip 2 → effective timeout = 100ms * 2 = 200ms (consecutive_trips=2, shift=1)
        cb.on_result(true, Some(&err));

        // Immediately after re-trip — rejected
        assert!(cb.on_entry().is_err());

        // Wait past second backoff (400ms)
        std::thread::sleep(Duration::from_millis(450));
        assert!(cb.on_entry().unwrap()); // probe allowed

        // trip_count: 1 (initial trip) + 1 (probe failure) = 2
        assert_eq!(cb.trip_count(), 2);
    }

    #[test]
    fn only_one_probe_under_concurrent_access() {
        let config = CircuitBreakerConfig {
            error_threshold: 1,
            open_timeout: Duration::ZERO,
            ..Default::default()
        };
        let cb = std::sync::Arc::new(CircuitBreaker::new(config, "node:6379".into()));

        // Trip the breaker
        assert!(cb.on_result(false, Some(&transport_err())));

        // Spawn 10 threads all calling on_entry() simultaneously
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let cb = cb.clone();
                std::thread::spawn(move || cb.on_entry())
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let probes = results.iter().filter(|r| matches!(r, Ok(true))).count();
        let rejections = results.iter().filter(|r| r.is_err()).count();

        // Exactly 1 probe allowed, rest rejected
        assert_eq!(probes, 1);
        assert_eq!(rejections, 9);
    }
}
