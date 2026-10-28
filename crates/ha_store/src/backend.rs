use crate::error::{Result, StoreError};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// The contract a real Redis client (or any other store) must satisfy.
/// The HA client never talks to Redis directly: it talks to this trait,
/// which is what makes the fake-Sentinel harness a faithful stand-in
/// for tests and CI. A production backend would implement the same
/// trait over redis-rs + Sentinel endpoint discovery.
#[allow(async_fn_in_trait)]
pub trait StoreBackend: Send + Sync {
    async fn put(&self, key: &str, value: &str, op_id: Uuid) -> Result<()>;
    async fn put_batch(&self, entries: &[(String, String)], op_id: Uuid) -> Result<()>;
    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
        op_id: Uuid,
    ) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn subscribe(&self, channel: &str) -> Result<()>;
    async fn resubscribe(&self, channels: &[String]) -> Result<()>;
    async fn reconnect(&self, primary: &str) -> Result<()>;
    fn live_subscriptions(&self) -> HashSet<String>;
    fn applied_operation_count(&self) -> u64;
}

/// The contract for endpoint discovery. Sentinel tells the client *who*
/// the primary is; it does not repair the client's connections or
/// subscriptions for it. That distinction is the whole point of this crate.
#[allow(async_fn_in_trait)]
pub trait Sentinel: Send + Sync {
    async fn current_primary(&self) -> Result<String>;
}

/// Shared, in-memory cluster state used by both the fake backend and the
/// fake Sentinel, mirroring how a real Redis primary + Sentinel pair
/// share one view of the topology.
#[derive(Debug, Default)]
struct ClusterInner {
    primary: String,
    /// False after a failover / drop until the client reconnects.
    connected: bool,
    partitioned: bool,
    /// Simulated reconnect latency (ms) after a failover.
    reconnect_delay_ms: u64,
    data: HashMap<String, String>,
    /// op_id -> () for every operation already committed. Replaying an
    /// op_id is a no-op, which is what makes post-failover retries safe.
    applied_ops: HashSet<Uuid>,
    live_subscriptions: HashSet<String>,
}

#[derive(Debug, Clone, Default)]
pub struct FakeCluster {
    inner: Arc<Mutex<ClusterInner>>,
}

impl FakeCluster {
    pub fn new(primary: &str) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClusterInner {
                primary: primary.to_string(),
                connected: true,
                ..Default::default()
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ClusterInner> {
        self.inner.lock().expect("cluster lock poisoned")
    }

    /// Simulate Sentinel promoting a new primary. Existing connections
    /// become unusable and live subscriptions are lost, exactly as in
    /// the real failure: the client must reconnect *and* resubscribe.
    pub fn failover_to(&self, new_primary: &str) {
        let mut c = self.lock();
        c.primary = new_primary.to_string();
        c.connected = false;
        c.live_subscriptions.clear();
    }

    /// Simulate a network partition: Sentinel is unreachable and the
    /// primary cannot be reached either.
    pub fn partition(&self) {
        let mut c = self.lock();
        c.partitioned = true;
        c.connected = false;
        c.live_subscriptions.clear();
    }

    pub fn heal(&self) {
        let mut c = self.lock();
        c.partitioned = false;
    }

    /// Simulate a dropped TCP connection without a primary change.
    pub fn drop_connection(&self) {
        let mut c = self.lock();
        c.connected = false;
        c.live_subscriptions.clear();
    }

    pub fn set_reconnect_delay_ms(&self, ms: u64) {
        self.lock().reconnect_delay_ms = ms;
    }

    pub fn primary(&self) -> String {
        self.lock().primary.clone()
    }

    pub fn is_connected(&self) -> bool {
        let c = self.lock();
        c.connected && !c.partitioned
    }
}

fn check_connected(c: &ClusterInner) -> Result<()> {
    if c.partitioned {
        return Err(StoreError::ConnectionLost("network partition".into()));
    }
    if !c.connected {
        return Err(StoreError::ConnectionLost(
            "connection to primary is gone (failover or drop)".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct FakeBackend {
    cluster: FakeCluster,
}

impl FakeBackend {
    pub fn new(cluster: FakeCluster) -> Self {
        Self { cluster }
    }
}

impl StoreBackend for FakeBackend {
    async fn put(&self, key: &str, value: &str, op_id: Uuid) -> Result<()> {
        let mut c = self.cluster.lock();
        check_connected(&c)?;
        // Idempotency: a retried op_id never applies twice.
        if !c.applied_ops.insert(op_id) {
            return Ok(());
        }
        c.data.insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn put_batch(&self, entries: &[(String, String)], op_id: Uuid) -> Result<()> {
        let mut c = self.cluster.lock();
        check_connected(&c)?;
        if !c.applied_ops.insert(op_id) {
            return Ok(());
        }
        for (k, v) in entries {
            c.data.insert(k.clone(), v.clone());
        }
        Ok(())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
        op_id: Uuid,
    ) -> Result<()> {
        let mut c = self.cluster.lock();
        check_connected(&c)?;
        if !c.applied_ops.insert(op_id) {
            return Ok(());
        }
        let current = c.data.get(key).map(|s| s.as_str());
        if current != expected {
            // Undo the op reservation: a failed precondition is not a
            // committed write, so a later retry with the same op_id
            // must be allowed to re-evaluate the condition.
            c.applied_ops.remove(&op_id);
            return Err(StoreError::ConditionFailed {
                key: key.to_string(),
            });
        }
        c.data.insert(key.to_string(), new_value.to_string());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let c = self.cluster.lock();
        check_connected(&c)?;
        Ok(c.data.get(key).cloned())
    }

    async fn subscribe(&self, channel: &str) -> Result<()> {
        let mut c = self.cluster.lock();
        check_connected(&c)?;
        c.live_subscriptions.insert(channel.to_string());
        Ok(())
    }

    async fn resubscribe(&self, channels: &[String]) -> Result<()> {
        let mut c = self.cluster.lock();
        check_connected(&c)?;
        // Insert into a set: resubscription is idempotent by construction.
        for ch in channels {
            c.live_subscriptions.insert(ch.clone());
        }
        Ok(())
    }

    async fn reconnect(&self, primary: &str) -> Result<()> {
        let delay = self.cluster.lock().reconnect_delay_ms;
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        let mut c = self.cluster.lock();
        if c.partitioned {
            return Err(StoreError::ConnectionLost("still partitioned".into()));
        }
        c.primary = primary.to_string();
        c.connected = true;
        c.live_subscriptions.clear(); // a new socket has no subscriptions yet
        Ok(())
    }

    fn live_subscriptions(&self) -> HashSet<String> {
        self.cluster.lock().live_subscriptions.clone()
    }

    fn applied_operation_count(&self) -> u64 {
        self.cluster.lock().applied_ops.len() as u64
    }
}

#[derive(Debug, Clone)]
pub struct FakeSentinel {
    cluster: FakeCluster,
}

impl FakeSentinel {
    pub fn new(cluster: FakeCluster) -> Self {
        Self { cluster }
    }
}

impl Sentinel for FakeSentinel {
    async fn current_primary(&self) -> Result<String> {
        let c = self.cluster.lock();
        if c.partitioned {
            return Err(StoreError::SentinelUnavailable(
                "sentinel unreachable during partition".into(),
            ));
        }
        Ok(c.primary.clone())
    }
}
