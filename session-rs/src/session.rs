//! Session/path split — QUIC-style stable logical identity + ephemeral transport path.
//!
//! ## Key invariant: stream_seq is session-scoped
//!
//! `stream_seq` is monotone within a session and continues across path migrations.
//! It resets to zero only on session *termination*, not on reconnect.
//!
//! ```text
//! path migration:   session_id stable, stream_seq continues from last_ack
//! session new:      new session_id, stream_seq starts at 0
//! ```
//!
//! ## Path states
//!
//! ```text
//! Active   — current primary path; records accepted and forwarded
//! Draining — previous path after migration; records with valid stream_seq still accepted
//! Closed   — fully retired; all new records from this path are rejected
//! ```

use rustc_hash::FxHashMap;
use pgress_core::partition::AuthorityMode;
use crate::{PathId, SessionId, TenantId};

// ── PathState ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathState {
    /// Current primary transport path for this session.
    Active,
    /// New path has taken over; this path is draining in-flight records.
    Draining,
    /// Fully retired. Records from this path are rejected.
    Closed,
}

// ── PathEntry ─────────────────────────────────────────────────────────────────

/// An ephemeral transport path bound to a session.
#[derive(Clone, Debug)]
pub struct PathEntry {
    pub path_id:             PathId,
    pub session_id:          SessionId,
    /// Last acknowledged stream_seq on this path.
    /// Inherited by the new path on migration so gap detection continues.
    pub last_ack_stream_seq: u64,
    pub state:               PathState,
}

// ── SessionEntry ──────────────────────────────────────────────────────────────

/// A stable logical session, independent of transport path.
#[derive(Clone, Debug)]
pub struct SessionEntry {
    pub session_id:     SessionId,
    pub tenant_id:      TenantId,
    /// Current active path. None during initial setup before first path binds.
    pub active_path_id: Option<PathId>,
    /// Previous path, draining after a migration. None if no migration has occurred.
    pub prev_path_id:   Option<PathId>,
    /// Records with `stream_seq < floor` are replays; suppress without forwarding.
    /// Set on session creation; advanced on explicit acknowledgement.
    pub stream_seq_floor: u64,
    pub auth_mode:      AuthorityMode,
}

// ── SessionError ──────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("session {0:?} not found")]
    NotFound(SessionId),
    #[error("path {0:?} not found")]
    PathNotFound(PathId),
}

// ── SessionTable ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct SessionTable(FxHashMap<SessionId, SessionEntry>);

impl SessionTable {
    pub fn new() -> Self { Self::default() }

    pub fn create(&mut self, entry: SessionEntry) {
        self.0.insert(entry.session_id, entry);
    }

    pub fn get(&self, id: SessionId) -> Option<&SessionEntry> {
        self.0.get(&id)
    }

    pub fn get_mut(&mut self, id: SessionId) -> Option<&mut SessionEntry> {
        self.0.get_mut(&id)
    }

    /// Terminate a session. The caller is responsible for draining associated paths.
    /// A new session with the same id starts with stream_seq_floor = 0.
    pub fn terminate(&mut self, id: SessionId) -> Option<SessionEntry> {
        self.0.remove(&id)
    }

    /// Migrate to a new path.
    ///
    /// - Marks the current active path as `Draining`.
    /// - Registers `new_path_id` as `Active` in `path_table`, inheriting
    ///   `last_ack_on_old` so stream_seq gap detection is continuous.
    /// - Updates `session.active_path_id` and `session.prev_path_id`.
    ///
    /// `stream_seq` is session-scoped and does NOT reset on migration.
    pub fn migrate_path(
        &mut self,
        session_id:    SessionId,
        new_path_id:   PathId,
        last_ack_on_old: u64,
        path_table:    &mut PathTable,
    ) -> Result<(), SessionError> {
        let session = self.0.get_mut(&session_id)
            .ok_or(SessionError::NotFound(session_id))?;

        // Mark old path as draining
        if let Some(old_path_id) = session.active_path_id {
            if let Some(old) = path_table.0.get_mut(&old_path_id) {
                old.state = PathState::Draining;
            }
            session.prev_path_id = Some(old_path_id);
        }

        // Register new path, inheriting last_ack so gap detection continues
        path_table.0.insert(new_path_id, PathEntry {
            path_id:             new_path_id,
            session_id,
            last_ack_stream_seq: last_ack_on_old,
            state:               PathState::Active,
        });

        session.active_path_id = Some(new_path_id);
        Ok(())
    }

    /// Retire the draining (previous) path once all in-flight records have been processed.
    pub fn retire_draining_path(
        &mut self,
        session_id: SessionId,
        path_table: &mut PathTable,
    ) -> Result<(), SessionError> {
        let session = self.0.get_mut(&session_id)
            .ok_or(SessionError::NotFound(session_id))?;

        if let Some(prev) = session.prev_path_id.take() {
            if let Some(entry) = path_table.0.get_mut(&prev) {
                entry.state = PathState::Closed;
            }
        }
        Ok(())
    }

    /// Returns true if `stream_seq` is valid for this session (>= floor).
    /// Records below the floor are replays and must be suppressed.
    pub fn is_seq_valid(&self, session_id: SessionId, stream_seq: u64) -> bool {
        match self.0.get(&session_id) {
            Some(s) => stream_seq >= s.stream_seq_floor,
            None    => false,
        }
    }

    /// Advance the stream_seq floor (called on explicit acknowledgement).
    pub fn ack_seq(&mut self, session_id: SessionId, acked_seq: u64) {
        if let Some(s) = self.0.get_mut(&session_id) {
            if acked_seq + 1 > s.stream_seq_floor {
                s.stream_seq_floor = acked_seq + 1;
            }
        }
    }

    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

// ── PathTable ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct PathTable(pub FxHashMap<PathId, PathEntry>);

impl PathTable {
    pub fn new() -> Self { Self::default() }

    pub fn create(&mut self, entry: PathEntry) {
        self.0.insert(entry.path_id, entry);
    }

    pub fn get(&self, id: PathId) -> Option<&PathEntry> {
        self.0.get(&id)
    }

    pub fn get_mut(&mut self, id: PathId) -> Option<&mut PathEntry> {
        self.0.get_mut(&id)
    }

    /// Resolve a path to its owning session. Returns None if path not found
    /// or path is Closed.
    pub fn resolve_session(&self, path_id: PathId) -> Option<SessionId> {
        self.0.get(&path_id).and_then(|e| {
            if e.state == PathState::Closed { None } else { Some(e.session_id) }
        })
    }

    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn base_session(session_id: u64, path_id: u64) -> (SessionEntry, PathEntry) {
        let sid = SessionId(session_id);
        let pid = PathId(path_id);
        let session = SessionEntry {
            session_id:       sid,
            tenant_id:        TenantId(1),
            active_path_id:   Some(pid),
            prev_path_id:     None,
            stream_seq_floor: 0,
            auth_mode:        AuthorityMode::Advisory,
        };
        let path = PathEntry {
            path_id:             pid,
            session_id:          sid,
            last_ack_stream_seq: 0,
            state:               PathState::Active,
        };
        (session, path)
    }

    #[test]
    fn stream_seq_continues_across_path_migration() {
        let mut sessions = SessionTable::new();
        let mut paths    = PathTable::new();

        let (sess, path_a) = base_session(1, 100);
        sessions.create(sess);
        // Simulate some acks on path_a
        paths.create(PathEntry { last_ack_stream_seq: 50, ..path_a });

        // Migrate to path_b
        sessions.migrate_path(SessionId(1), PathId(200), 50, &mut paths).unwrap();

        // Old path is draining
        assert_eq!(paths.get(PathId(100)).unwrap().state, PathState::Draining);
        // New path is active and inherits last_ack
        let new_path = paths.get(PathId(200)).unwrap();
        assert_eq!(new_path.state,               PathState::Active);
        assert_eq!(new_path.last_ack_stream_seq, 50);   // inherited, not reset
        // Session points at new path
        assert_eq!(sessions.get(SessionId(1)).unwrap().active_path_id, Some(PathId(200)));
        assert_eq!(sessions.get(SessionId(1)).unwrap().prev_path_id,   Some(PathId(100)));
    }

    #[test]
    fn stream_seq_resets_on_session_termination() {
        let mut sessions = SessionTable::new();
        let mut paths    = PathTable::new();

        let (sess, path) = base_session(2, 300);
        sessions.create(sess);
        paths.create(path);
        sessions.ack_seq(SessionId(2), 99);
        assert_eq!(sessions.get(SessionId(2)).unwrap().stream_seq_floor, 100);

        // Terminate session
        sessions.terminate(SessionId(2));

        // Create a new session with the same id — floor starts at 0
        sessions.create(SessionEntry {
            session_id:       SessionId(2),
            tenant_id:        TenantId(1),
            active_path_id:   None,
            prev_path_id:     None,
            stream_seq_floor: 0,   // fresh start
            auth_mode:        AuthorityMode::Advisory,
        });
        assert_eq!(sessions.get(SessionId(2)).unwrap().stream_seq_floor, 0);
    }

    #[test]
    fn replay_suppression_below_floor() {
        let mut sessions = SessionTable::new();
        let (sess, _) = base_session(3, 400);
        sessions.create(sess);
        sessions.ack_seq(SessionId(3), 9);  // floor = 10

        assert!(!sessions.is_seq_valid(SessionId(3), 9));  // below floor → replay
        assert!(sessions.is_seq_valid(SessionId(3), 10));  // at floor → valid
        assert!(sessions.is_seq_valid(SessionId(3), 11));  // above floor → valid
    }

    #[test]
    fn closed_path_does_not_resolve_session() {
        let mut paths = PathTable::new();
        paths.create(PathEntry {
            path_id:             PathId(1),
            session_id:          SessionId(1),
            last_ack_stream_seq: 0,
            state:               PathState::Closed,
        });
        assert!(paths.resolve_session(PathId(1)).is_none());
    }

    #[test]
    fn active_path_resolves_session() {
        let mut paths = PathTable::new();
        paths.create(PathEntry {
            path_id:             PathId(2),
            session_id:          SessionId(5),
            last_ack_stream_seq: 0,
            state:               PathState::Active,
        });
        assert_eq!(paths.resolve_session(PathId(2)), Some(SessionId(5)));
    }

    #[test]
    fn migrate_path_unknown_session_returns_error() {
        let mut sessions = SessionTable::new();
        let mut paths    = PathTable::new();
        let result = sessions.migrate_path(SessionId(999), PathId(1), 0, &mut paths);
        assert!(result.is_err());
    }
}
