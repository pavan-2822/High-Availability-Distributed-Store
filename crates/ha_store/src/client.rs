use crate::backend::{Sentinel, StoreBackend};
use crate::error::{Result, StoreError};
use crate::guard::RecoveryGuard;
use crate::state::{Backoff, ClientState, Metrics, WritePath};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

/// Async HA storage client.
///
/// Recovery sequence, in order, every time:
/// 1. Failure observed on a write path -> state `FailureDetected`.
/// 2. Recovery coordinated -> UUID guard admits exactly one attempt.
/// 3. Endpoint recovered -> Sentinel names the current primary.
/// 4. Connection recovered -> reconnect to that primary.
/// 5. Subscriptions restored -> resubscribe every desired channel,
///    idempotently, *before* reporting healthy.
/// 6. Writes resume. Idempotent writes retry once with their original
///    op_id; the backend's op log makes that retry a no-op if the
///    first attempt already committed.
pub struct HaStoreClient<B, S> {
    backend: Arc<B>,
    sentinel: Arc<S>,
    state: Mutex<ClientState>,
    desired_subscriptions: Mutex<HashSet<String>>,
    guard: RecoveryGuard,
    metrics: Mutex<Metrics>,
    backoff: Backoff,
    last_recovery_id: Mutex<Option<Uuid>>,
}

impl<B, S> HaStoreClient<B, S>
where
    B: StoreBackend,
    S: Sentinel,
{
    pub fn new(backend: Arc<B>, sentinel: Arc<S>) -> Self {
        Self {
            backend,
            sentinel,
            state: Mutex::new(ClientState::Healthy),
            desired_subscriptions: Mutex::new(HashSet::new()),
            guard: RecoveryGuard::new(),
            metrics: Mutex::new(Metrics::default()),
            backoff: Backoff::default(),
            last_recovery_id: Mutex::new(None),
        }
    }

    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn state(&self) -> ClientState {
        *self.state.lock().expect("state lock poisoned")
    }

    pub fn metrics(&self) -> Metrics {
        let mut m = self.metrics.lock().expect("metrics lock poisoned").clone();
        m.applied_operation_count = self.backend.applied_operation_count();
        m
    }

    pub fn last_recovery_id(&self) -> Option<Uuid> {
        *self.last_recovery_id.lock().expect("id lock poisoned")
    }

    fn set_state(&self, s: ClientState) {
        *self.state.lock().expect("state lock poisoned") = s;
    }

    fn record_failure(&self, path: WritePath) {
        self.metrics
            .lock()
            .expect("metrics lock poisoned")
            .record_failure(path);
        self.set_state(ClientState::FailureDetected);
    }

    /// Track a desired subscription durably in the client, then subscribe
    /// on the live connection. The desired set -- not the live set -- is
    /// the source of truth after a reconnect.
    pub async fn subscribe(&self, channel: &str) -> Result<()> {
        self.desired_subscriptions
            .lock()
            .expect("subs lock poisoned")
            .insert(channel.to_string());
        match self.backend.subscribe(channel).await {
            Ok(()) => Ok(()),
            Err(StoreError::ConnectionLost(_)) => {
                // Subscription will be restored by recovery; record it
                // as a failure so it is visible, but do not fail the
                // caller's intent to stay subscribed.
                self.recover().await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn desired_subscriptions(&self) -> HashSet<String> {
        self.desired_subscriptions
            .lock()
            .expect("subs lock poisoned")
            .clone()
    }

    /// What the backend currently has live (post-resubscribe). Tests
    /// use this to prove a healthy socket is not mistaken for restored
    /// subscriptions.
    pub fn live_subscriptions(&self) -> HashSet<String> {
        self.backend.live_subscriptions()
    }

    // ---- Write path 1: single key ------------------------------------
    pub async fn put(&self, key: &str, value: &str) -> Result<()> {
        let op_id = Uuid::new_v4();
        match self.backend.put(key, value, op_id).await {
            Ok(()) => Ok(()),
            Err(StoreError::ConnectionLost(_)) => {
                self.record_failure(WritePath::Single);
                self.recover().await?;
                // Retry with the *same* op_id: safe even if the first
                // attempt committed before the connection dropped.
                self.backend.put(key, value, op_id).await
            }
            Err(e) => Err(e),
        }
    }

    // ---- Write path 2: batch -----------------------------------------
    pub async fn put_batch(&self, entries: &[(String, String)]) -> Result<()> {
        let op_id = Uuid::new_v4();
        match self.backend.put_batch(entries, op_id).await {
            Ok(()) => Ok(()),
            Err(StoreError::ConnectionLost(_)) => {
                self.record_failure(WritePath::Batch);
                self.recover().await?;
                self.backend.put_batch(entries, op_id).await
            }
            Err(e) => Err(e),
        }
    }

    // ---- Write path 3: conditional (compare-and-swap) ------------------
    pub async fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<()> {
        let op_id = Uuid::new_v4();
        match self
            .backend
            .compare_and_swap(key, expected, new_value, op_id)
            .await
        {
            Ok(()) => Ok(()),
            Err(StoreError::ConnectionLost(_)) => {
                self.record_failure(WritePath::Conditional);
                self.recover().await?;
                // CAS re-evaluates its condition on retry, so a changed
                // value between attempts correctly yields ConditionFailed
                // instead of a silent overwrite.
                self.backend
                    .compare_and_swap(key, expected, new_value, op_id)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// A deliberately non-idempotent write path, kept to demonstrate the
    /// ambiguous-write policy: on a connection loss it does NOT recover
    /// and does NOT retry. The caller must reconcile (check the value,
    /// or retry with an op_id via `put`) instead of assuming success.
    pub async fn put_without_idempotency(&self, key: &str, value: &str) -> Result<()> {
        let op_id = Uuid::new_v4();
        match self.backend.put(key, value, op_id).await {
            Ok(()) => Ok(()),
            Err(StoreError::ConnectionLost(_)) => Err(StoreError::AmbiguousWrite {
                key: key.to_string(),
            }),
            Err(e) => Err(e),
        }
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        self.backend.get(key).await
    }

    /// Run one guarded recovery. Concurrent callers are deduplicated:
    /// only the first caller performs the work, the rest observe
    /// `RecoveryAlreadyInProgress` and can poll `state()` / metrics.
    pub async fn recover(&self) -> Result<()> {
        let id = match self.guard.begin() {
            Some(id) => id,
            None => {
                self.metrics
                    .lock()
                    .expect("metrics lock poisoned")
                    .duplicate_recovery_suppressed += 1;
                return Err(StoreError::RecoveryAlreadyInProgress);
            }
        };
        // finish() is called on *every* exit below, so a failed recovery
        // never leaves the guard stuck.
        let started = Instant::now();
        self.set_state(ClientState::Reconnecting);
        let outcome = self.recover_inner().await;
        match &outcome {
            Ok(()) => {
                let latency = started.elapsed().as_millis() as u64;
                let mut m = self.metrics.lock().expect("metrics lock poisoned");
                m.recovery_attempts += 1;
                m.successful_recoveries += 1;
                m.last_recovery_latency_ms = Some(latency);
                *self.last_recovery_id.lock().expect("id lock poisoned") = Some(id);
            }
            Err(_) => {
                let mut m = self.metrics.lock().expect("metrics lock poisoned");
                m.recovery_attempts += 1;
            }
        }
        self.guard.finish(id);
        outcome
    }

    async fn recover_inner(&self) -> Result<()> {
        for attempt in 0..self.backoff.max_attempts {
            self.set_state(ClientState::Reconnecting);
            let primary = match self.sentinel.current_primary().await {
                Ok(p) => p,
                Err(_) => {
                    if attempt + 1 >= self.backoff.max_attempts {
                        self.set_state(ClientState::RetryExhausted);
                        return Err(StoreError::RetryExhausted);
                    }
                    tokio::time::sleep(self.backoff.delay_for(attempt)).await;
                    continue;
                }
            };
            match self.backend.reconnect(&primary).await {
                Ok(()) => {
                    // Healthy socket != healthy client. Restore subscriptions
                    // before reporting healthy.
                    self.set_state(ClientState::Resubscribing);
                    let desired: Vec<String> = self
                        .desired_subscriptions
                        .lock()
                        .expect("subs lock poisoned")
                        .iter()
                        .cloned()
                        .collect();
                    self.backend.resubscribe(&desired).await?;
                    {
                        let mut m = self.metrics.lock().expect("metrics lock poisoned");
                        m.resubscriptions_restored += desired.len() as u64;
                    }
                    self.set_state(ClientState::Healthy);
                    return Ok(());
                }
                Err(_) => {
                    if attempt + 1 >= self.backoff.max_attempts {
                        self.set_state(ClientState::RetryExhausted);
                        return Err(StoreError::RetryExhausted);
                    }
                    tokio::time::sleep(self.backoff.delay_for(attempt)).await;
                }
            }
        }
        self.set_state(ClientState::RetryExhausted);
        Err(StoreError::RetryExhausted)
    }
}
