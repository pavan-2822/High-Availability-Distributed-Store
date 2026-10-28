//! Raft durable state. Depends only on `consensus_types`.
//!
//! Deliberately *not* on `raft_api`: state can be tested, persisted,
//! and reasoned about without the RPC surface, and that one-way
//! dependency is what breaks the old cycle (see `consensus_types`).

use consensus_types::{LogEntry, NodeId, Term};

#[derive(Debug)]
pub struct RaftState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            current_term: Term(0),
            voted_for: None,
            log: Vec::new(),
        }
    }
}

impl RaftState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append only if the entry's term is not stale. Returns false
    /// (and appends nothing) for a stale term, instead of corrupting
    /// the log with an entry from an old leader.
    pub fn append(&mut self, entry: LogEntry) -> bool {
        if entry.term < self.current_term {
            return false;
        }
        self.log.push(entry);
        true
    }

    pub fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_entries_are_not_appended() {
        let mut s = RaftState {
            current_term: Term(5),
            ..Default::default()
        };
        assert!(!s.append(LogEntry::new(Term(3), 1, b"x")));
        assert!(s.log.is_empty());
        assert!(s.append(LogEntry::new(Term(5), 1, b"x")));
        assert_eq!(s.last_index(), 1);
    }
}
