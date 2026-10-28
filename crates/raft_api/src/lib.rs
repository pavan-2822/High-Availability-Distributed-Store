//! Raft API surface. Depends only on `consensus_types`.
//!
//! Deliberately *not* on `raft_state`: the API must be usable by
//! clients and tests without pulling in durable-state implementation
//! details, and that one-way dependency is what breaks the old cycle.

use consensus_types::{LogEntry, NodeId, Term, VoteRequest, VoteResponse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    StaleTerm { current: Term },
    NotLeader,
}

/// The RPC surface a Raft node exposes. Implementations live elsewhere;
/// this trait is the contract, defined purely over shared types.
pub trait RaftApi: Send + Sync {
    fn request_vote(&self, req: VoteRequest) -> Result<VoteResponse, ApiError>;
    fn append_entries(
        &self,
        term: Term,
        leader: NodeId,
        entries: Vec<LogEntry>,
    ) -> Result<(), ApiError>;
    fn current_term(&self) -> Term;
}

/// A minimal in-memory implementation, useful for tests of callers.
/// Real persistence and networking are intentionally out of scope here.
pub struct InMemoryRaftApi {
    term: std::sync::Mutex<Term>,
}

impl InMemoryRaftApi {
    pub fn new(term: Term) -> Self {
        Self {
            term: std::sync::Mutex::new(term),
        }
    }
}

impl RaftApi for InMemoryRaftApi {
    fn request_vote(&self, req: VoteRequest) -> Result<VoteResponse, ApiError> {
        let current = *self.term.lock().unwrap();
        if req.term < current {
            return Err(ApiError::StaleTerm { current });
        }
        Ok(VoteResponse {
            term: req.term,
            vote_granted: true,
        })
    }

    fn append_entries(
        &self,
        term: Term,
        _leader: NodeId,
        _entries: Vec<LogEntry>,
    ) -> Result<(), ApiError> {
        let current = *self.term.lock().unwrap();
        if term < current {
            return Err(ApiError::StaleTerm { current });
        }
        Ok(())
    }

    fn current_term(&self) -> Term {
        *self.term.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus_types::Term;

    #[test]
    fn stale_term_is_rejected() {
        let api = InMemoryRaftApi::new(Term(5));
        let req = VoteRequest {
            term: Term(3),
            candidate: NodeId(2),
            last_log_index: 0,
            last_log_term: Term(0),
        };
        assert!(matches!(
            api.request_vote(req),
            Err(ApiError::StaleTerm { current: Term(5) })
        ));
    }
}
