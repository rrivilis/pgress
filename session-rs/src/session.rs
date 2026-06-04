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

    // ── Expiry tracking ───────────────────────────────────────────────────────

    /// Causal epoch of the last record admitted through this session.
    ///
    /// Updated by the session runtime each time a record passes all gates.
    /// Trigger 1 (inactivity): if `current_epoch - last_active_at > threshold`,
    /// the session is eligible for tombstoning.
    pub last_active_causal_epoch: u64,

    /// Consecutive causal epochs during which ALL of this session's partitions
    /// reported quiescence (empty propagation queue, all cells at fixed point).
    ///
    /// Incremented by `SessionRuntime::notify_domain_quiescent` when the domain
    /// belongs to this session AND the session already had a prior quiescent epoch.
    /// Reset to zero on any admitted record or partial-quiescence break.
    ///
    /// Trigger 3 (Zero-frustration): if `consecutive_quiescent_epochs >=
    /// zero_frustration_threshold`, the session is eligible for tombstoning.
    /// Analogous to `gated_trunk_cycles` saturating at 0x1FFF in the RTL.
    pub consecutive_quiescent_epochs: u32,
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

    /// Record that a record was admitted for `session_id` at `causal_epoch`.
    ///
    /// Updates `last_active_causal_epoch` and resets `consecutive_quiescent_epochs`
    /// (an admitted record breaks any Zero-frustration streak).
    pub fn record_activity(&mut self, session_id: SessionId, causal_epoch: u64) {
        if let Some(s) = self.0.get_mut(&session_id) {
            if causal_epoch > s.last_active_causal_epoch {
                s.last_active_causal_epoch = causal_epoch;
            }
            s.consecutive_quiescent_epochs = 0;
        }
    }

    /// Increment the consecutive-quiescence counter for `session_id`.
    ///
    /// Called by `SessionRuntime::notify_domain_quiescent` when the quiescent
    /// domain belongs to this session. Saturates at `u32::MAX`.
    pub fn increment_quiescent_epochs(&mut self, session_id: SessionId) {
        if let Some(s) = self.0.get_mut(&session_id) {
            s.consecutive_quiescent_epochs =
                s.consecutive_quiescent_epochs.saturating_add(1);
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

// ── SessionReaper ─────────────────────────────────────────────────────────────

/// Policy for tombstoning sessions that have become idle or permanently frustrated.
///
/// Three triggers, applied in order during `SessionReaper::reap`:
///
/// 1. **Inactivity timeout** (`inactivity_epoch_threshold`): session's
///    `last_active_causal_epoch` is more than this many epochs behind the
///    supplied `current_epoch`. Catches sessions that stopped sending records.
///
/// 2. **Epoch lag** (`epoch_lag_threshold`): session's
///    `last_active_causal_epoch` is more than this many epochs behind the
///    `max_known_epoch` seen across all live sessions. Catches sessions that
///    are actively sending but falling behind the causal frontier (e.g. stale
///    producers whose values no longer matter to downstream consumers).
///
/// 3. **Zero-frustration** (`zero_frustration_threshold`): session's
///    `consecutive_quiescent_epochs` meets or exceeds this value. Catches
///    sessions whose entire output space has been at the Bochvar-Zero
///    (conflicted) fixed point for too long — the hardware analog of
///    `gated_trunk_cycles` saturating in `ternary_region.sv`.
///    `None` disables this trigger.
///
/// `None` for a numeric threshold disables that trigger entirely.
#[derive(Clone, Debug)]
pub struct ExpiryPolicy {
    /// Trigger 1: epochs since last activity before tombstoning. `None` = disabled.
    pub inactivity_epoch_threshold: Option<u64>,
    /// Trigger 2: max causal lag behind frontier before tombstoning. `None` = disabled.
    pub epoch_lag_threshold: Option<u64>,
    /// Trigger 3: consecutive quiescent epochs before tombstoning.
    /// Hardware analog: `gated_trunk_cycles` saturation limit (0x1FFF in RTL).
    /// `None` = disabled (default for most deployments that want to preserve
    /// quiescent sessions until explicitly terminated).
    pub zero_frustration_threshold: Option<u32>,
}

impl Default for ExpiryPolicy {
    fn default() -> Self {
        ExpiryPolicy {
            inactivity_epoch_threshold: Some(10_000),
            epoch_lag_threshold:        Some(50_000),
            zero_frustration_threshold: None,  // opt-in only
        }
    }
}

/// Why a session was tombstoned by the reaper.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryReason {
    /// Trigger 1: session was inactive for too many causal epochs.
    Inactivity { last_active: u64, current: u64 },
    /// Trigger 2: session fell too far behind the causal frontier.
    EpochLag { last_active: u64, frontier: u64 },
    /// Trigger 3: session output was at Zero for too many consecutive epochs.
    ZeroFrustration { consecutive_epochs: u32 },
}

/// Result of a single reaping run.
#[derive(Clone, Debug, Default)]
pub struct ReapResult {
    /// Sessions tombstoned by this run, with the reason for each.
    pub tombstoned: Vec<(SessionId, ExpiryReason)>,
}

/// Applies the configured `ExpiryPolicy` against all live sessions.
#[derive(Clone, Debug, Default)]
pub struct SessionReaper {
    pub policy: ExpiryPolicy,
}

impl SessionReaper {
    pub fn new(policy: ExpiryPolicy) -> Self { SessionReaper { policy } }

    /// Scan `table` and terminate sessions that violate the policy.
    ///
    /// `current_epoch`: the highest causal epoch the caller considers "now".
    ///
    /// Returns a `ReapResult` listing every session that was tombstoned and why.
    /// The caller should propagate tombstoned session IDs to the `PathTable`,
    /// `DomainRegistry`, and any dependent state.
    pub fn reap(&self, table: &mut SessionTable, current_epoch: u64) -> ReapResult {
        // Compute the max known epoch across all live sessions (for trigger 2).
        let frontier: u64 = table.0.values()
            .map(|s| s.last_active_causal_epoch)
            .max()
            .unwrap_or(current_epoch);

        let mut tombstoned = Vec::new();

        // Collect session IDs to avoid borrowing issues.
        let ids: Vec<SessionId> = table.0.keys().copied().collect();

        for session_id in ids {
            let reason = {
                let s = match table.0.get(&session_id) {
                    Some(s) => s,
                    None    => continue,
                };

                // Trigger 1: inactivity
                if let Some(thresh) = self.policy.inactivity_epoch_threshold {
                    if current_epoch.saturating_sub(s.last_active_causal_epoch) > thresh {
                        Some(ExpiryReason::Inactivity {
                            last_active: s.last_active_causal_epoch,
                            current:     current_epoch,
                        })
                    } else { None }
                } else { None }
                .or_else(|| {
                    // Trigger 2: epoch lag
                    if let Some(thresh) = self.policy.epoch_lag_threshold {
                        if frontier.saturating_sub(s.last_active_causal_epoch) > thresh {
                            return Some(ExpiryReason::EpochLag {
                                last_active: s.last_active_causal_epoch,
                                frontier,
                            });
                        }
                    }
                    None
                })
                .or_else(|| {
                    // Trigger 3: zero frustration
                    if let Some(thresh) = self.policy.zero_frustration_threshold {
                        if s.consecutive_quiescent_epochs >= thresh {
                            return Some(ExpiryReason::ZeroFrustration {
                                consecutive_epochs: s.consecutive_quiescent_epochs,
                            });
                        }
                    }
                    None
                })
            };

            if let Some(reason) = reason {
                table.terminate(session_id);
                tombstoned.push((session_id, reason));
            }
        }

        ReapResult { tombstoned }
    }
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
            last_active_causal_epoch:    0,
            consecutive_quiescent_epochs: 0,
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
            last_active_causal_epoch:     0,
            consecutive_quiescent_epochs: 0,
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

    // ── SessionReaper tests ───────────────────────────────────────────────────

    fn make_session(id: u64, last_epoch: u64, quiescent_epochs: u32) -> SessionEntry {
        SessionEntry {
            session_id:       SessionId(id),
            tenant_id:        TenantId(0),
            active_path_id:   None,
            prev_path_id:     None,
            stream_seq_floor: 0,
            auth_mode:        AuthorityMode::Advisory,
            last_active_causal_epoch:     last_epoch,
            consecutive_quiescent_epochs: quiescent_epochs,
        }
    }

    #[test]
    fn reaper_inactivity_trigger() {
        let policy = ExpiryPolicy {
            inactivity_epoch_threshold: Some(100),
            epoch_lag_threshold:        None,
            zero_frustration_threshold: None,
        };
        let reaper = SessionReaper::new(policy);
        let mut table = SessionTable::new();
        table.create(make_session(1, 0, 0));   // inactive for > 100 epochs
        table.create(make_session(2, 950, 0)); // last active recently

        let result = reaper.reap(&mut table, 1_000);
        assert_eq!(result.tombstoned.len(), 1);
        assert_eq!(result.tombstoned[0].0, SessionId(1));
        assert!(matches!(result.tombstoned[0].1, ExpiryReason::Inactivity { .. }));
        // Session 2 survives
        assert!(table.get(SessionId(2)).is_some());
    }

    #[test]
    fn reaper_epoch_lag_trigger() {
        let policy = ExpiryPolicy {
            inactivity_epoch_threshold: None,
            epoch_lag_threshold:        Some(500),
            zero_frustration_threshold: None,
        };
        let reaper = SessionReaper::new(policy);
        let mut table = SessionTable::new();
        // frontier = max(100, 1000) = 1000; session 1 lag = 900 > 500
        table.create(make_session(1, 100, 0));
        table.create(make_session(2, 1000, 0));

        let result = reaper.reap(&mut table, 1_000);
        assert_eq!(result.tombstoned.len(), 1);
        assert_eq!(result.tombstoned[0].0, SessionId(1));
        assert!(matches!(result.tombstoned[0].1, ExpiryReason::EpochLag { .. }));
    }

    #[test]
    fn reaper_zero_frustration_trigger() {
        let policy = ExpiryPolicy {
            inactivity_epoch_threshold: None,
            epoch_lag_threshold:        None,
            zero_frustration_threshold: Some(50),
        };
        let reaper = SessionReaper::new(policy);
        let mut table = SessionTable::new();
        table.create(make_session(1, 0, 75));  // 75 >= 50 → tombstone
        table.create(make_session(2, 0, 10));  // 10 < 50 → survive

        let result = reaper.reap(&mut table, 100);
        assert_eq!(result.tombstoned.len(), 1);
        assert!(matches!(
            result.tombstoned[0].1,
            ExpiryReason::ZeroFrustration { consecutive_epochs: 75 }
        ));
        assert!(table.get(SessionId(2)).is_some());
    }

    #[test]
    fn reaper_disabled_triggers_never_tombstone() {
        let policy = ExpiryPolicy {
            inactivity_epoch_threshold: None,
            epoch_lag_threshold:        None,
            zero_frustration_threshold: None,
        };
        let reaper = SessionReaper::new(policy);
        let mut table = SessionTable::new();
        // Session that would be tombstoned by every trigger if enabled
        table.create(make_session(1, 0, u32::MAX));

        let result = reaper.reap(&mut table, u64::MAX);
        assert!(result.tombstoned.is_empty(), "all triggers disabled → no tombstoning");
    }

    #[test]
    fn record_activity_resets_frustration_counter() {
        let mut table = SessionTable::new();
        table.create(make_session(1, 100, 42)); // has 42 consecutive quiescent epochs
        table.record_activity(SessionId(1), 200);
        let s = table.get(SessionId(1)).unwrap();
        assert_eq!(s.consecutive_quiescent_epochs, 0, "activity must reset frustration counter");
        assert_eq!(s.last_active_causal_epoch, 200);
    }

    #[test]
    fn increment_quiescent_epochs_saturates() {
        let mut table = SessionTable::new();
        table.create(make_session(1, 0, u32::MAX));
        table.increment_quiescent_epochs(SessionId(1)); // must not panic / overflow
        assert_eq!(table.get(SessionId(1)).unwrap().consecutive_quiescent_epochs, u32::MAX);
    }
}
