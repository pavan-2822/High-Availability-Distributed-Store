use std::sync::Mutex;
use uuid::Uuid;

/// UUID-guarded reconnect deduplication.
///
/// Several failure events often arrive close together (one per write
/// path, one per connection, one per subscription). Without a guard,
/// each would launch its own reconnect, racing to replace the same
/// connection and resubscribe the same channels.
///
/// Rules, stated so a reviewer can check them:
/// * `begin()` returns `Some(new_uuid)` only if no recovery is running.
///   Otherwise it returns `None`: the caller was deduplicated.
/// * The UUID identifies exactly one recovery attempt. A stale UUID can
///   never finish a later recovery, because `finish()` only clears the
///   guard when the id matches the running one.
/// * `finish()` always clears the guard, on success *and* on failure.
///   A guard that is never cleared would suppress every legitimate
///   later recovery -- the failure mode this guard must not introduce.
///   The client calls `finish()` in every exit path of `recover()`.
#[derive(Debug, Default)]
pub struct RecoveryGuard {
    current: Mutex<Option<Uuid>>,
}

impl RecoveryGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(&self) -> Option<Uuid> {
        let mut slot = self.current.lock().expect("guard lock poisoned");
        if slot.is_some() {
            return None;
        }
        let id = Uuid::new_v4();
        *slot = Some(id);
        Some(id)
    }

    /// Returns true only if `id` was the running recovery.
    pub fn finish(&self, id: Uuid) -> bool {
        let mut slot = self.current.lock().expect("guard lock poisoned");
        if *slot == Some(id) {
            *slot = None;
            true
        } else {
            false
        }
    }

    pub fn is_running(&self) -> bool {
        self.current.lock().expect("guard lock poisoned").is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_begin_is_deduplicated_until_finish() {
        let g = RecoveryGuard::new();
        let first = g.begin().expect("first recovery starts");
        assert!(g.begin().is_none());
        assert!(g.is_running());
        assert!(g.finish(first));
        assert!(!g.is_running());
        // A finished guard must allow a fresh, distinct recovery.
        let second = g.begin().expect("second recovery starts after finish");
        assert_ne!(first, second);
    }

    #[test]
    fn stale_uuid_cannot_finish_a_later_recovery() {
        let g = RecoveryGuard::new();
        let first = g.begin().unwrap();
        assert!(g.finish(first));
        let second = g.begin().unwrap();
        assert!(!g.finish(first), "stale id must not clear the new recovery");
        assert!(g.is_running());
        assert!(g.finish(second));
    }
}
