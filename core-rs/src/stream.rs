//! Remote event streams — the distribution boundary protocol.
//!
//! Event streams cross the partition boundary; dependencies do not.
//!
//! A `RemoteStream<T>` is a versioned, filtered sequence of observations
//! from a remote partition. Subscriptions declare `(Scope, MaskId, Sensitivity)`:
//! the emitting partition runs `affects` once per unique mask and delivers
//! only matching events. Network traffic is proportional to matching events,
//! not total graph mutations.
//!
//! ## Boundary invariant (I9)
//!
//! The `emit` function is the only path across the boundary. It enforces:
//! - `Pos`/`Neg` values: emitted directly as `RemotePayload::Value`
//! - Unresolved `Zero`: run `stabilize`; emit as `RemotePayload::TypedZero(k)`
//!   if still unresolved, or `RemotePayload::Value` if resolved
//! - Pending (not yet computed): emitted as `RemotePayload::Pending`
//!
//! Raw unstable interpretation never crosses.

use smallvec::SmallVec;
use crate::{
    mask::MaskId,
    partition::{PartitionId, PayloadKind, ZeroKind},
    sensitivity::Sensitivity,
    time::{Frontier, Time},
    uid::Uid,
};

/// A unique identifier for a stream (one per subscription).
pub type StreamId = uuid::Uuid;

// ── Scope — what a subscription attaches to ──────────────────────────────────

/// What a subscription observes: a specific node, a set of nodes, all nodes
/// of a given type, or an entire partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// A single node by UID.
    Uid(Uid),
    /// An explicit set of nodes (typically short — SmallVec optimised).
    UidSet(SmallVec<[Uid; 4]>),
    /// All nodes in a remote partition (coarse; use with a narrow mask).
    Partition(PartitionId),
    /// All nodes of a given `typ` string in a remote partition.
    TypedNodes { partition: PartitionId, typ: String },
}

impl Scope {
    pub fn uid(u: Uid) -> Self { Scope::Uid(u) }

    pub fn uid_set(uids: impl IntoIterator<Item = Uid>) -> Self {
        Scope::UidSet(uids.into_iter().collect())
    }

    /// Whether this scope covers a given (uid, typ) pair.
    pub fn matches(&self, uid: Uid, typ: &str, partition: PartitionId) -> bool {
        match self {
            Scope::Uid(u)                       => *u == uid,
            Scope::UidSet(us)                   => us.contains(&uid),
            Scope::Partition(p)                 => *p == partition,
            Scope::TypedNodes { partition: p, typ: t } => *p == partition && t == typ,
        }
    }
}

// ── Subscription ─────────────────────────────────────────────────────────────

/// A registered subscription at the emitting partition.
///
/// The triple `(scope, mask_id, sensitivity)` is the subscription key.
/// Two subscribers with the same key share event delivery — the `affects`
/// check runs once per unique mask, not once per subscriber.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    /// What nodes this subscription observes.
    pub scope:       Scope,
    /// Interned `ProjectionMask` ID (structural filter).
    pub mask_id:     MaskId,
    /// How to interpret observable changes (semantic filter).
    pub sensitivity: Sensitivity,
    /// The stream this subscription delivers to.
    pub stream_id:   StreamId,
}

// ── RemoteEvent ───────────────────────────────────────────────────────────────

/// A single observation delivered in a `RemoteStream`.
///
/// Carries:
/// - `time`:     the product timestamp of the change at the emitting partition
/// - `uid`:      which node changed
/// - `payload`:  the definite value (Some = Pos or Neg; None = Neg/pending)
/// - `zero`:     typed uncertainty signal (Some = emitted Zero with named reason)
/// - `frontier`: the emitting partition's current frontier — used for GC
///               (receiver can release references older than this frontier)
#[derive(Clone, Debug)]
pub struct RemoteEvent<T> {
    pub time:     Time,
    pub uid:      Uid,
    pub payload:  Option<T>,
    pub zero:     Option<ZeroKind>,
    pub frontier: Frontier,
}

impl<T> RemoteEvent<T> {
    /// Construct a definite-value event.
    pub fn value(uid: Uid, val: T, time: Time, frontier: Frontier) -> Self {
        RemoteEvent { time, uid, payload: Some(val), zero: None, frontier }
    }

    /// Construct a typed Zero event.
    pub fn typed_zero(uid: Uid, kind: ZeroKind, time: Time, frontier: Frontier) -> Self {
        RemoteEvent { time, uid, payload: None, zero: Some(kind), frontier }
    }

    /// Construct a pending (not yet computed) event.
    pub fn pending(uid: Uid, time: Time, frontier: Frontier) -> Self {
        RemoteEvent { time, uid, payload: None, zero: None, frontier }
    }

    /// Whether this event carries a typed Zero signal.
    pub fn is_zero(&self) -> bool { self.zero.is_some() }

    /// Whether this event indicates the node is pending (no value, no Zero).
    pub fn is_pending(&self) -> bool { self.payload.is_none() && self.zero.is_none() }
}

// ── RemoteStream ──────────────────────────────────────────────────────────────

/// A versioned event stream from a remote partition, filtered by a subscription.
///
/// Events are delivered in causal order (advancing `frontier`). The receiver
/// consumes events via iteration; when the receiver's acknowledged frontier
/// advances past an event's frontier, the emitter can compact that epoch.
#[derive(Clone, Debug)]
pub struct RemoteStream<T> {
    pub origin:    PartitionId,
    pub stream_id: StreamId,
    pub events:    Vec<RemoteEvent<T>>,
}

impl<T> RemoteStream<T> {
    pub fn new(origin: PartitionId) -> Self {
        RemoteStream {
            origin,
            stream_id: StreamId::new_v4(),
            events: Vec::new(),
        }
    }

    pub fn push(&mut self, event: RemoteEvent<T>) {
        self.events.push(event);
    }

    /// Drain all events, returning them.
    pub fn drain(&mut self) -> Vec<RemoteEvent<T>> {
        std::mem::take(&mut self.events)
    }

    pub fn is_empty(&self) -> bool { self.events.is_empty() }
    pub fn len(&self) -> usize { self.events.len() }
}

// ── RemotePayload — what emit() produces ─────────────────────────────────────

/// The stabilized output of the `emit` function — what crosses the boundary.
///
/// Only one of these three forms may cross; raw unstable interpretation
/// (live Zero without a reason) may not.
#[derive(Clone, Debug)]
pub enum RemotePayload<T> {
    /// A definite value: the emitting partition stabilized to Pos or Neg.
    Value(T, RemoteMetadata),
    /// A typed uncertainty: the emitting partition could not stabilize and
    /// forwards its named uncertainty for the receiver to interpret.
    TypedZero(ZeroKind, RemoteMetadata),
    /// Pending: the node has not yet been computed (deps unresolved).
    Pending(RemoteMetadata),
}

/// Provenance metadata attached to every boundary emission.
#[derive(Clone, Debug)]
pub struct RemoteMetadata {
    pub source_partition: PartitionId,
    pub source_uid:       Uid,
    pub source_version:   u64,
    pub causal_frontier:  crate::time::VectorClock,
}

impl RemoteMetadata {
    pub fn payload_kind(&self, payload: &RemotePayload<impl Clone>) -> PayloadKind {
        match payload {
            RemotePayload::Value(..)     => PayloadKind::Definite,
            RemotePayload::TypedZero(k, _) => PayloadKind::TypedZero(*k),
            RemotePayload::Pending(..)   => PayloadKind::Definite, // treated as Neg
        }
    }
}

// ── emit ─────────────────────────────────────────────────────────────────────

/// The boundary stabilization gate.
///
/// Before a value crosses a partition boundary, `emit` ensures it is in a
/// resolved form. It takes the current value and version of a node and
/// produces the appropriate `RemotePayload`.
///
/// Policy:
/// - `Pos` / `Neg`: emit directly as `Value`
/// - `Zero` with a known reason: emit as `TypedZero(k)` — do not block
/// - `Zero` without a known reason: caller must run `Stabilize` first and
///   pass the resulting `ZeroKind`; or use `EmitPolicy::Eager` to emit
///   `TypedZero(Incomplete)` immediately
/// - Pending (value is `Neg` and deps haven't resolved): emit as `Pending`
pub fn emit<T: Clone>(
    uid:            Uid,
    version:        u64,
    value:          T,           // the node's current T value (caller maps T to payload)
    zero_kind:      Option<ZeroKind>,  // Some if value is Zero with known reason
    is_pending:     bool,        // true if node's value is Neg and not yet computed
    partition:      PartitionId,
    clock:          crate::time::VectorClock,
    _boundary_time: u64,
) -> RemotePayload<T> {
    let metadata = RemoteMetadata {
        source_partition: partition,
        source_uid: uid,
        source_version: version,
        causal_frontier: clock,
    };

    if is_pending {
        return RemotePayload::Pending(metadata);
    }

    match zero_kind {
        Some(k) => RemotePayload::TypedZero(k, metadata),
        None    => RemotePayload::Value(value, metadata),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{time::{ProductTime, VectorClock}, uid};

    fn frontier() -> Frontier { Frontier::new() }
    fn t0() -> Time { ProductTime::ZERO }

    #[test]
    fn test_remote_event_variants() {
        let id = uid::fresh();
        let ev_val  = RemoteEvent::value(id, 1i32, t0(), frontier());
        let ev_zero: RemoteEvent<i32> = RemoteEvent::typed_zero(id, ZeroKind::Conflict, t0(), frontier());
        let ev_pend: RemoteEvent<i32> = RemoteEvent::pending(id, t0(), frontier());

        assert!(!ev_val.is_zero());  assert!(!ev_val.is_pending());
        assert!(ev_zero.is_zero());  assert!(!ev_zero.is_pending());
        assert!(!ev_pend.is_zero()); assert!(ev_pend.is_pending());
    }

    #[test]
    fn test_remote_stream_push_drain() {
        let pid = PartitionId::new_v4();
        let mut stream: RemoteStream<i32> = RemoteStream::new(pid);
        let id = uid::fresh();
        stream.push(RemoteEvent::value(id, 42, t0(), frontier()));
        stream.push(RemoteEvent::typed_zero(id, ZeroKind::Retracted, t0(), frontier()));
        assert_eq!(stream.len(), 2);
        let events = stream.drain();
        assert_eq!(events.len(), 2);
        assert!(stream.is_empty());
    }

    #[test]
    fn test_emit_definite_value() {
        let id = uid::fresh();
        let pid = PartitionId::new_v4();
        let result = emit(id, 3u64, 42i32, None, false, pid, VectorClock::new(), 1);
        assert!(matches!(result, RemotePayload::Value(42, _)));
    }

    #[test]
    fn test_emit_typed_zero() {
        let id = uid::fresh();
        let pid = PartitionId::new_v4();
        let result: RemotePayload<i32> = emit(
            id, 3, 0, Some(ZeroKind::Conflict), false, pid, VectorClock::new(), 1
        );
        assert!(matches!(result, RemotePayload::TypedZero(ZeroKind::Conflict, _)));
    }

    #[test]
    fn test_emit_pending() {
        let id = uid::fresh();
        let pid = PartitionId::new_v4();
        let result: RemotePayload<i32> = emit(
            id, 0, -1, None, true, pid, VectorClock::new(), 0
        );
        assert!(matches!(result, RemotePayload::Pending(_)));
    }

    #[test]
    fn test_scope_matches() {
        let id = uid::fresh();
        let pid = PartitionId::new_v4();
        assert!(Scope::Uid(id).matches(id, "input", pid));
        assert!(!Scope::Uid(uid::fresh()).matches(id, "input", pid));
        assert!(Scope::Partition(pid).matches(id, "anything", pid));
        assert!(Scope::TypedNodes { partition: pid, typ: "sensor".into() }
            .matches(uid::fresh(), "sensor", pid));
        assert!(!Scope::TypedNodes { partition: pid, typ: "sensor".into() }
            .matches(uid::fresh(), "actuator", pid));
    }
}
