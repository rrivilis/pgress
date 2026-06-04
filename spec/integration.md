# pgress Integration Guide

> **Audience:** Integrators wiring pgress into a host application or network service.
> This document covers the high/low split, the Dispatcher pattern, end-to-end
> pipeline, bootstrap semantics, and known gaps.

---

## The high/low split

`SessionRuntime` (session-rs) is the **gate layer** — a stateless predicate applied
to each record header. It does not drive execution, own engine state, or parse record
bodies. It produces a routing decision and hands off.

The **Dispatcher** is the driver. It owns the full pipeline and is implemented
in `session-rs/src/dispatch.rs`:

```rust
pub struct Dispatcher {
    pub decoder: StreamDecoder,    // wire bytes → ParsedHeader + RecordAction
    pub runtime: SessionRuntime,   // gate: ParsedHeader → RouteOutcome
    pub shards:  EnginePool,       // ShardId → pgress_core::Engine
}

// EnginePool is a HashMap<ShardId, Engine> with get-or-create semantics.
// Currently all partitions route to ShardId(0) (single bootstrap shard).
// Multi-shard placement is a future enhancement.
```

The session layer **never** touches `IsaOp` or engine state. Its output is
`RouteOutcome::Admitted { opcode_class }`. The Dispatcher owns body parsing
(`parse_payload`) and engine dispatch (`EnginePool::apply`).

---

## End-to-end pipeline

```text
raw bytes (TCP / QUIC / file)
  │
  ├─ StreamDecoder::decode_preamble(buf: &[u8; 32])
  │      → StreamPreamble  (magic, abi_version, path_id, session_id)
  │
  ├─ StreamDecoder::decode_record(buf: &[u8; 48], &runtime)
  │      → RecordAction::RouteReady(ParsedHeader)
  │      → RecordAction::ReadProfile { payload_len }   [opcode 0x0100]
  │      → RecordAction::SkipPayload { skip_bytes }    [unknown opcode]
  │
  ├─ [if ReadProfile]
  │    StreamDecoder::process_profile(&header, payload, &mut runtime)
  │      → clips policy to tenant ceiling (all four axes)
  │      → creates/updates session in runtime.domains
  │      → registers path in runtime.paths  [bootstrap path]
  │
  ├─ SessionRuntime::route_record(&header)
  │      → RouteOutcome::Admitted { opcode_class }
  │      → RouteOutcome::Rejected(RejectionReason)
  │
  ├─ [if Admitted]
  │    shard_id = runtime.domains.partitions[header.partition_id].shard_id
  │    op      = parse_payload(header.opcode, payload_bytes)   [integrator]
  │    outcome = shards[shard_id].apply(op)                    [integrator]
  │    runtime.update_pressure(shard_id, outcome.pressure)
  │
  └─ [if EngineOutcome::BudgetExhausted]
       recovery: requeue WorkCursor or escalate per FailurePolicy
```

`parse_payload` (body deserialiser keyed on opcode) is the integrator's
responsibility. It is the inverse of the payload encoding rules in `spec/abi.md`.

---

## Hot path vs. slow lane

`opcode_class` from `route_record` drives the dispatch path:

| OpcodeClass  | Path        | Contention model                        |
|--------------|-------------|-----------------------------------------|
| `Control`    | Slow lane   | Per-shard serialised queue (mutex OK)   |
| `SetValue`   | Hot path    | Per-shard lock-free channel             |
| `Propagate`  | Hot path    | Per-shard lock-free channel             |
| `Demand`     | Hot path    | Per-shard lock-free channel             |
| `Stabilize`  | Slow lane   | Per-shard serialised (e-graph work)     |
| `Unknown`    | Skip        | Payload skipped; not dispatched         |

The session layer already classifies; the Dispatcher routes by it.

---

## Bootstrap

### Local bootstrap (trusted in-process)

The host creates sessions directly before any stream arrives:

```rust
runtime.sessions.create(SessionEntry {
    session_id:                   SessionId(id),
    tenant_id:                    TenantId(tenant),
    active_path_id:               Some(PathId(path)),
    prev_path_id:                 None,
    stream_seq_floor:             0,
    auth_mode:                    AuthorityMode::Advisory,
    last_active_causal_epoch:     0,
    consecutive_quiescent_epochs: 0,
});
runtime.paths.create(PathEntry {
    path_id:             PathId(path),
    session_id:          SessionId(id),
    last_ack_stream_seq: 0,
    state:               PathState::Active,
});
```

Session is immediately `Active`. No wire handshake required.

### Remote bootstrap (Advisory and Asserted)

The peer sends opcode `0x0100` (`SessionProfile`) as the first record on a
fresh path. `process_profile` creates the session and registers the path:

```
peer → [IsaStreamHeader | IsaHeader(0x0100) | SessionProfile payload]
         ↓
host: decode_record → RecordAction::ReadProfile
         ↓
host: process_profile → session created + path registered (atomic)
         ↓
subsequent records admitted normally
```

**Tenant must be pre-registered.** If `TenantId` from the profile is not in
`runtime.domains.tenants`, all four policy axes are silently clipped to `NONE`.
Production deployments should register tenants before accepting remote streams
or explicitly reject on missing tenant.

**ASSERTED paths.** If `trust_level = 0x02`, `process_profile` calls
`verify_asserted` before creating or updating the session. `verify_asserted`
looks up the issuer's `VerifyingKey` from `runtime.trust_store` and verifies the
Ed25519 signature over the 64-byte canonical payload:
`assertion_mask ‖ delegation_mask ‖ observability_mask ‖ disclosure_mask ‖
issuer_domain ‖ session_id ‖ generation ‖ expiry_epoch` (all little-endian, 8
bytes each). A missing key in `trust_store` is treated as a verification failure.
The session is not created until verification succeeds; a failed ASSERTED profile
returns `IngressError::SignatureVerificationFailed`. Add issuer keys via:

```rust
runtime.trust_store.insert(TenantId(issuer_id), verifying_key);
```

**Re-bootstrap with existing session_id** (reconnect on new path): if
`process_profile` finds the session already `Active`, register the new path
and update `active_path_id` — do not recreate the session. The existing
`stream_seq_floor` is preserved; `stream_seq` continues from where it left off.

### Rejection reasons on unbootstrapped paths

| Reason                | Meaning                                                      |
|-----------------------|--------------------------------------------------------------|
| `SessionNotFound`     | No session registered — hello not yet received or rejected   |
| `UnauthenticatedPath` | Session exists but path admission pending (ASSERTED path awaiting verification) |

A client receiving `SessionNotFound` should send a `SessionProfile` hello.
A client receiving `UnauthenticatedPath` should wait for bootstrap to complete.

---

## Trust levels

| Level      | Status            | Notes                                                         |
|------------|-------------------|---------------------------------------------------------------|
| ADVISORY   | Production-ready  | No signature; policy clipped to parent tenant ceiling on all four axes |
| ASSERTED   | Production-ready  | Ed25519 over 64-byte canonical payload (four-mask form); `trust_store: FxHashMap<TenantId, VerifyingKey>` is a field of `SessionRuntime`; generation-based revocation is O(1) |
| ATTESTED   | Not yet implemented | Stream key exchange during bootstrap; per-record MAC required |

---

## Known gaps

| Gap      | Impact                          | Notes                                                       |
|----------|---------------------------------|-------------------------------------------------------------|
| ATTESTED | Stream MAC not implemented      | Requires bootstrap key exchange; per-record MAC under derived session key |
