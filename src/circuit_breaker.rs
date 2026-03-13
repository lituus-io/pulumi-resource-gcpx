//! Circuit breaker for cascading failure protection.
//!
//! Wraps GCP API calls to prevent sustained retries during outages.
//! States: Closed (normal) → Open (failing, fast-reject) → HalfOpen (probe).

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

pub struct CircuitBreaker {
    failure_threshold: u32,
    success_threshold: u32,
    timeout: Duration,
    state: AtomicU8,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    last_failure: AtomicU64,
}

#[derive(Debug, thiserror::Error)]
#[error("circuit breaker open: too many failures, retry after timeout")]
pub struct CircuitOpen;

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, success_threshold: u32, timeout: Duration) -> Self {
        Self {
            failure_threshold,
            success_threshold,
            timeout,
            state: AtomicU8::new(STATE_CLOSED),
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            last_failure: AtomicU64::new(0),
        }
    }

    /// Default circuit breaker: open after 5 failures, close after 2 successes,
    /// try half-open after 30s.
    pub fn default_config() -> Self {
        Self::new(5, 2, Duration::from_secs(30))
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Check if the circuit allows a request through.
    pub fn allow_request(&self) -> Result<(), CircuitOpen> {
        match self.state.load(Ordering::Acquire) {
            STATE_CLOSED => Ok(()),
            STATE_HALF_OPEN => Ok(()), // allow probe
            STATE_OPEN => {
                // Check if timeout has elapsed → transition to half-open
                let last = self.last_failure.load(Ordering::Relaxed);
                let elapsed = Self::now_secs().saturating_sub(last);
                if elapsed >= self.timeout.as_secs() {
                    self.state.store(STATE_HALF_OPEN, Ordering::Release);
                    self.success_count.store(0, Ordering::Relaxed);
                    Ok(())
                } else {
                    Err(CircuitOpen)
                }
            }
            _ => Ok(()),
        }
    }

    /// Record a successful operation.
    pub fn record_success(&self) {
        match self.state.load(Ordering::Acquire) {
            STATE_HALF_OPEN => {
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.success_threshold {
                    self.state.store(STATE_CLOSED, Ordering::Release);
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                }
            }
            STATE_CLOSED => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a failed operation.
    pub fn record_failure(&self) {
        self.last_failure.store(Self::now_secs(), Ordering::Relaxed);

        match self.state.load(Ordering::Acquire) {
            STATE_CLOSED => {
                let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.failure_threshold {
                    self.state.store(STATE_OPEN, Ordering::Release);
                    eprintln!(
                        "gcpx: circuit breaker OPEN after {} consecutive failures",
                        count
                    );
                }
            }
            STATE_HALF_OPEN => {
                // Probe failed → back to open
                self.state.store(STATE_OPEN, Ordering::Release);
                self.success_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_OPEN
    }

    pub fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_CLOSED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_closed() {
        let cb = CircuitBreaker::default_config();
        assert!(cb.is_closed());
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn opens_after_threshold() {
        let cb = CircuitBreaker::new(3, 1, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_closed());
        cb.record_failure();
        assert!(cb.is_open());
        assert!(cb.allow_request().is_err());
    }

    #[test]
    fn success_resets_failure_count() {
        let cb = CircuitBreaker::new(3, 1, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_closed()); // still closed, only 2 consecutive
    }

    #[test]
    fn half_open_closes_on_success() {
        let cb = CircuitBreaker::new(2, 1, Duration::from_secs(0));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_open());
        // timeout=0 so immediately transitions to half-open
        assert!(cb.allow_request().is_ok());
        cb.record_success();
        assert!(cb.is_closed());
    }
}
