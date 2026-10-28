use std::fmt;

/// Every failure a caller can act on. Connection failures are recoverable
/// (the client can reconnect); ambiguous writes are deliberately *not*
/// auto-retried, because the client cannot know whether Redis committed
/// the write before the connection disappeared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The connection to the current primary is gone or unusable.
    ConnectionLost(String),
    /// Sentinel cannot currently tell us who the primary is.
    SentinelUnavailable(String),
    /// Recovery was attempted the configured number of times and gave up.
    RetryExhausted,
    /// Another recovery is already running; this call was deduplicated.
    RecoveryAlreadyInProgress,
    /// A write was sent, then the connection dropped, and its outcome is
    /// unknown. Retrying blindly could duplicate a non-idempotent operation.
    AmbiguousWrite { key: String },
    /// A compare-and-swap precondition did not hold. Not a connection error.
    ConditionFailed { key: String },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::ConnectionLost(m) => write!(f, "connection lost: {m}"),
            StoreError::SentinelUnavailable(m) => write!(f, "sentinel unavailable: {m}"),
            StoreError::RetryExhausted => write!(f, "recovery retries exhausted"),
            StoreError::RecoveryAlreadyInProgress => {
                write!(f, "recovery already in progress (deduplicated)")
            }
            StoreError::AmbiguousWrite { key } => {
                write!(f, "ambiguous write outcome for key {key}: not retried")
            }
            StoreError::ConditionFailed { key } => {
                write!(f, "compare-and-swap condition failed for key {key}")
            }
        }
    }
}

impl std::error::Error for StoreError {}

pub type Result<T> = std::result::Result<T, StoreError>;
