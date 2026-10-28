//! Run: `cargo run -p ha_store --example failover_demo`
//!
//! Walks the exact story from the README: write, failover, write again,
//! and shows that subscriptions survive the failover too.

use ha_store::{FakeBackend, FakeCluster, FakeSentinel, HaStoreClient};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let cluster = FakeCluster::new("redis-primary:6379");
    let client = Arc::new(HaStoreClient::new(
        Arc::new(FakeBackend::new(cluster.clone())),
        Arc::new(FakeSentinel::new(cluster.clone())),
    ));

    client.subscribe("jobs").await.unwrap();
    client.put("task:42", "pending").await.unwrap();
    println!(
        "before failover: {:?}",
        client.get("task:42").await.unwrap()
    );

    cluster.failover_to("redis-replica-1:6379");
    println!(
        "failover to replica-1; live subs now: {:?}",
        client.live_subscriptions()
    );

    client.put("task:42", "running").await.unwrap();
    println!(
        "after recovery:  {:?}",
        client.get("task:42").await.unwrap()
    );
    println!("live subs restored: {:?}", client.live_subscriptions());
    println!(
        "state: {:?}, metrics: {:?}",
        client.state(),
        client.metrics()
    );
}
