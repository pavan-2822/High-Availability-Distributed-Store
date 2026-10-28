//! High-availability async Rust storage client.
//!
//! The core lesson, stated plainly: **a Redis failover does not
//! guarantee that the application client has recovered.** Sentinel
//! coordinates which Redis instance is primary. The client must still
//! detect the failure, coordinate a single reconnect, reconnect to the
//! new primary, and restore its subscriptions. This crate makes each
//! of those steps explicit, observable, and testable.
//!
//! Start with [`client::HaStoreClient`], the fake harness in
//! [`backend::FakeCluster`], and the ten failover scenarios in
//! `tests/failover.rs`.

pub mod backend;
pub mod client;
pub mod error;
pub mod guard;
pub mod state;

pub use backend::{FakeBackend, FakeCluster, FakeSentinel, Sentinel, StoreBackend};
pub use client::HaStoreClient;
pub use error::{Result, StoreError};
pub use guard::RecoveryGuard;
pub use state::{Backoff, ClientState, Metrics, WritePath};
