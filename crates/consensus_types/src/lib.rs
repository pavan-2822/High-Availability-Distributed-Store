//! Shared Raft types, owned by no other Raft crate.
//!
//! Refactor boundary: previously, the Raft API types and the Raft
//! state types depended on each other, which created a compiler-level
//! circular dependency that blocked the Raft API from compiling at all.
//! The fix is a boundary, not a bigger crate: move the types both sides
//! need into this leaf crate, and let `raft_api` and `raft_state` each
//! depend *only* on `consensus_types`. Neither depends on the other, so
//! no cycle can form, and either crate can change internally without
//! forcing a rebuild / review of the other side's API surface.

/// A node in the Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u64);

/// A Raft term. Terms only increase, which is what makes stale leaders
/// detectable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Term(pub u64);

/// One entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: u64,
    /// Opaque command bytes, as stored. Interpretation belongs to the
    /// state machine, not to the consensus layer.
    pub command: Vec<u8>,
}

impl LogEntry {
    pub fn new(term: Term, index: u64, command: impl Into<Vec<u8>>) -> Self {
        Self {
            term,
            index,
            command: command.into(),
        }
    }
}

/// RequestVote RPC payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate: NodeId,
    pub last_log_index: u64,
    pub last_log_term: Term,
}

/// RequestVote RPC response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terms_order_and_log_entries_hold() {
        assert!(Term(2) > Term(1));
        let e = LogEntry::new(Term(1), 0, b"put k v");
        assert_eq!(e.index, 0);
        assert_eq!(e.command, b"put k v");
    }
}
