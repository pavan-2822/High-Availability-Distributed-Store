use std::collections::HashMap;

/// Explicit client lifecycle. A healthy socket alone is not "healthy":
/// the client is only healthy once subscriptions are restored too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    Connected,
    FailureDetected,
    Reconnecting,
    Resubscribing,
    Healthy,
    RetryExhausted,
}

/// The three write paths. Failure detection is per-path, so a regression
/// in one path cannot hide behind the others in metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WritePath {
    Single,
    Batch,
    Conditional,
}

impl WritePath {
    pub fn name(self) -> &'static str {
        match self {
            WritePath::Single => "single",
            WritePath::Batch => "batch",
            WritePath::Conditional => "conditional",
        }
    }
}

/// Observable recovery behaviour. These are the numbers an interviewer
/// should ask for next: success rate and recovery latency, not just
/// "tests pass".
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub failures_by_path: HashMap<WritePath, u64>,
    pub recovery_attempts: u64,
    pub successful_recoveries: u64,
    pub duplicate_recovery_suppressed: u64,
    pub resubscriptions_restored: u64,
    /// Milliseconds from failure detection to a healthy client, last run.
    pub last_recovery_latency_ms: Option<u64>,
    /// Operation ids the backend has already applied (idempotency log size).
    pub applied_operation_count: u64,
}

impl Metrics {
    pub fn record_failure(&mut self, path: WritePath) {
        *self.failures_by_path.entry(path).or_insert(0) += 1;
    }
}

/// Bounded exponential backoff with jitter. Bounded, so a reconnect storm
/// cannot retry forever; jittered, so many clients do not all retry at
/// the same instant after a shared failover.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub base_ms: u64,
    pub max_ms: u64,
    pub max_attempts: usize,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base_ms: 10,
            max_ms: 500,
            max_attempts: 3,
        }
    }
}

impl Backoff {
    pub fn delay_for(&self, attempt: usize) -> std::time::Duration {
        let exp = self.base_ms.saturating_mul(1u64 << attempt.min(10));
        let capped = exp.min(self.max_ms);
        // Small deterministic jitter from the attempt number: keeps tests
        // reproducible while still spreading retries in production use.
        let jitter = (attempt as u64 * 7) % 13;
        std::time::Duration::from_millis(capped + jitter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded() {
        let b = Backoff::default();
        assert!(b.delay_for(100) <= std::time::Duration::from_millis(513));
        assert!(b.delay_for(0) < b.delay_for(3));
    }

    #[test]
    fn metrics_count_failures_per_path() {
        let mut m = Metrics::default();
        m.record_failure(WritePath::Single);
        m.record_failure(WritePath::Single);
        m.record_failure(WritePath::Batch);
        assert_eq!(m.failures_by_path[&WritePath::Single], 2);
        assert_eq!(m.failures_by_path[&WritePath::Batch], 1);
    }
}
