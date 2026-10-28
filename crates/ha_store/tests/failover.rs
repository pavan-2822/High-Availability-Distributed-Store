//! Fake-Sentinel failover harness: ten deterministic failure scenarios.
//!
//! No real Redis is required. Every scenario drives `FakeCluster`
//! through a failure, then asserts the exact client behaviour that
//! matters: data written, exactly once where idempotency applies,
//! subscriptions restored, and no duplicate recovery work.
//!
//! Run: `cargo test -p ha_store --test failover`

use ha_store::{ClientState, FakeBackend, FakeCluster, FakeSentinel, HaStoreClient};
use std::sync::Arc;

fn client() -> (FakeCluster, Arc<HaStoreClient<FakeBackend, FakeSentinel>>) {
    let cluster = FakeCluster::new("redis-primary:6379");
    let backend = Arc::new(FakeBackend::new(cluster.clone()));
    let sentinel = Arc::new(FakeSentinel::new(cluster.clone()));
    (cluster, Arc::new(HaStoreClient::new(backend, sentinel)))
}

// 1. A single write survives a Sentinel failover.
#[tokio::test]
async fn single_write_recovers_after_failover() {
    let (cluster, c) = client();
    c.put("k", "v1").await.unwrap();
    cluster.failover_to("redis-replica-1:6379");
    c.put("k", "v2").await.unwrap();
    assert_eq!(c.get("k").await.unwrap().as_deref(), Some("v2"));
    assert_eq!(c.state(), ClientState::Healthy);
    assert_eq!(c.metrics().successful_recoveries, 1);
}

// 2. A batch write survives a Sentinel failover, atomically per op_id.
#[tokio::test]
async fn batch_write_recovers_after_failover() {
    let (cluster, c) = client();
    cluster.failover_to("redis-replica-1:6379");
    c.put_batch(&[
        ("a".to_string(), "1".to_string()),
        ("b".to_string(), "2".to_string()),
    ])
    .await
    .unwrap();
    assert_eq!(c.get("a").await.unwrap().as_deref(), Some("1"));
    assert_eq!(c.get("b").await.unwrap().as_deref(), Some("2"));
}

// 3. A conditional write survives a failover and still honours its condition.
#[tokio::test]
async fn cas_write_recovers_after_failover() {
    let (cluster, c) = client();
    c.put("counter", "0").await.unwrap();
    cluster.failover_to("redis-replica-1:6379");
    c.compare_and_swap("counter", Some("0"), "1").await.unwrap();
    assert_eq!(c.get("counter").await.unwrap().as_deref(), Some("1"));
    // A stale condition must still fail, even after recovery.
    assert!(c.compare_and_swap("counter", Some("0"), "2").await.is_err());
    assert_eq!(c.get("counter").await.unwrap().as_deref(), Some("1"));
}

// 4. Ten concurrent failure events trigger exactly one recovery.
#[tokio::test]
async fn concurrent_failures_trigger_only_one_recovery() {
    let (cluster, c) = client();
    // Hold the guard open long enough that all 10 callers overlap.
    cluster.set_reconnect_delay_ms(100);
    let mut handles = Vec::new();
    for _ in 0..10 {
        let cc = c.clone();
        handles.push(tokio::spawn(async move { cc.recover().await }));
    }
    let mut ok = 0;
    let mut deduped = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(()) => ok += 1,
            Err(ha_store::StoreError::RecoveryAlreadyInProgress) => deduped += 1,
            Err(e) => panic!("unexpected recovery error: {e}"),
        }
    }
    // Note: with no failure injected, recover() still runs the guarded
    // sequence once; the point is the count, not the trigger.
    assert_eq!(ok, 1);
    assert_eq!(deduped, 9);
    assert_eq!(c.metrics().successful_recoveries, 1);
    assert_eq!(c.metrics().duplicate_recovery_suppressed, 9);
}

// 5. Subscriptions are restored after reconnect, without duplicates.
#[tokio::test]
async fn subscriptions_restored_after_reconnect_without_duplicates() {
    let (cluster, c) = client();
    c.subscribe("jobs").await.unwrap();
    c.subscribe("results").await.unwrap();
    cluster.failover_to("redis-replica-1:6379");
    c.put("k", "v").await.unwrap(); // triggers recovery + resubscribe
    let live = c.live_subscriptions();
    assert!(live.contains("jobs") && live.contains("results"));
    assert_eq!(live.len(), 2, "resubscribe must be idempotent");
    cluster.failover_to("redis-replica-2:6379");
    c.put("k", "v2").await.unwrap();
    let live = c.live_subscriptions();
    assert_eq!(live.len(), 2);
}

// 6. A network partition exhausts bounded retries and reports it.
#[tokio::test]
async fn network_partition_exhausts_retries() {
    let (cluster, c) = client();
    cluster.partition();
    let err = c
        .recover()
        .await
        .expect_err("partitioned recovery must fail");
    assert_eq!(err, ha_store::StoreError::RetryExhausted);
    assert_eq!(c.state(), ClientState::RetryExhausted);
    // The guard must be cleared even on failure, or no later recovery
    // could ever run. Heal, then prove a fresh recovery works.
    cluster.heal();
    c.recover().await.unwrap();
    assert_eq!(c.state(), ClientState::Healthy);
}

// 7. A delayed reconnect still recovers, and latency is recorded.
#[tokio::test]
async fn delayed_reconnect_still_recovers() {
    let (cluster, c) = client();
    cluster.set_reconnect_delay_ms(150);
    cluster.failover_to("redis-replica-1:6379");
    c.put("k", "v").await.unwrap();
    assert_eq!(c.state(), ClientState::Healthy);
    let latency = c.metrics().last_recovery_latency_ms.expect("latency set");
    assert!(latency >= 150, "latency {latency} should include the delay");
}

// 8. A dropped connection mid-write retries exactly once, via op_id.
#[tokio::test]
async fn dropped_connection_mid_write_retries_exactly_once() {
    let (cluster, c) = client();
    cluster.drop_connection();
    c.put("k", "v").await.unwrap();
    assert_eq!(c.get("k").await.unwrap().as_deref(), Some("v"));
    // One logical write, one applied operation: no duplicate effect.
    assert_eq!(c.metrics().applied_operation_count, 1);
}

// 9. An ambiguous (non-idempotent) write is NOT blindly retried.
#[tokio::test]
async fn ambiguous_write_is_not_blindly_retried() {
    let (cluster, c) = client();
    cluster.drop_connection();
    let err = c
        .put_without_idempotency("k", "v")
        .await
        .expect_err("ambiguous write must surface, not retry");
    assert!(matches!(err, ha_store::StoreError::AmbiguousWrite { .. }));
    // No recovery was launched for this path, and nothing was written.
    assert_eq!(c.metrics().successful_recoveries, 0);
    assert_eq!(c.metrics().applied_operation_count, 0);
}

// 10. Repeated failovers each get a fresh recovery UUID (guard resets).
#[tokio::test]
async fn repeated_failovers_each_get_fresh_recovery() {
    let (cluster, c) = client();
    cluster.failover_to("redis-replica-1:6379");
    c.put("k", "1").await.unwrap();
    let first = c.last_recovery_id().expect("first recovery id");
    cluster.failover_to("redis-replica-2:6379");
    c.put("k", "2").await.unwrap();
    let second = c.last_recovery_id().expect("second recovery id");
    assert_ne!(first, second, "guard must reset between failovers");
    assert_eq!(c.metrics().successful_recoveries, 2);
    assert_eq!(c.get("k").await.unwrap().as_deref(), Some("2"));
}
