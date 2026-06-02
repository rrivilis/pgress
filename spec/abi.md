# pgress Binary ABI Specification

> **Status:** Draft — wire format locked, calling conventions provisional until ≥2 real consumers.
>
> **Scope:** This document defines the *binary wire encoding* for pgress ISA operations.
> It does NOT define calling conventions (that is the psABI, to be frozen later).
> The wire format is the stable artifact; the C/Zig/FFI function signatures are provisional.

---

## Design principles

| Principle | Rationale |
|-----------|-----------|
| Fixed-width opcodes | Binary-searchable streams, no framing ambiguity |
| Stable discriminants | Append-only: new opcodes never reuse old tags |
| Opcode ≠ feature level | Older runtimes can skip unknown opcodes without mis-parsing |
| Canonical encoding | Identical ISA sequences produce identical byte streams; replay is deterministic at two granularities — total order within each partition (`stream_seq`), partial order across partitions (edge-interaction causality) |
| Explicit handles | All node/edge identity via opaque u64 Uid; no raw pointers cross the boundary |
| No language assumptions | Pure C layout; no vtables, no ARC, no GC roots |
| Observable convergence | Scheduling and interleaving may vary internally; externally observable node interpretations must converge identically across runtimes and schedulers |

---

## IsaStreamHeader

Every stream (file, socket, mmap region) begins with a fixed 32-byte preamble. This appears
exactly once, before any `IsaHeader` records. Tools and parsers MUST check the magic and
reject streams with unknown `abi_version` values rather than attempting to parse blindly.

```c
struct IsaStreamHeader {
    char     magic[4];       // "PGRS" (0x50 0x47 0x52 0x53) — not null-terminated
    uint16_t abi_version;    // wire format version; currently 0x0003
    uint16_t flags;          // stream-level flags (see below)
    uint64_t feature_flags;  // capability bitmask negotiated for this stream
    uint64_t path_id;        // ephemeral transport path identity (QUIC-style)
    uint64_t session_id;     // stable logical session identity (mirrors IsaHeader.session_id)
};
// 32 bytes total.
```

- **`magic`** is the four bytes `P G R S` (0x50 0x47 0x52 0x53). A receiver that reads
  different bytes MUST treat the stream as invalid and stop parsing.
- **`abi_version`** identifies the wire format revision. Currently `0x0003` (32-byte
  `IsaStreamHeader` with `path_id` and `session_id`; 48-byte `IsaHeader` with
  `tenant_id`, `session_id`, `partition_id`, `causal_epoch`, `stream_seq`). The
  previous revision `0x0002` used a 16-byte `IsaStreamHeader` without path routing
  fields; revision `0x0001` used a 16-byte `IsaHeader`. Streams with
  `abi_version=0x0001` or `0x0002` MUST be handled by legacy code paths or rejected. A
  receiver that does not support the declared version MUST reject the stream; it MUST NOT
  attempt partial parsing. Future versions increment this field; a bump indicates a
  breaking wire change (which should be rare given the append-only opcode policy).
- **`path_id`** identifies the ephemeral transport path over which this stream is
  delivered. Path identity is QUIC-style: it is decoupled from logical session identity
  so that a session can survive transport interruptions and path migrations without
  breaking stream continuity. The session manager maps `path_id → session_id` via the
  `PathTable`; the engine never sees `path_id`. A new `path_id` on the same `session_id`
  signals a path migration or reconnect; `stream_seq` is NOT reset on path migration (see
  `IsaHeader.stream_seq`).
- **`session_id`** duplicates `IsaHeader.session_id` at the stream level so that the
  session manager can perform path-to-session binding from `IsaStreamHeader` alone,
  before parsing any `IsaHeader` records. This field MUST match `IsaHeader.session_id`
  for all records in the stream; a mismatch is a wire error.
- **`flags`** are stream-level modifiers:

  | Bit | Name | Meaning |
  |-----|------|---------|
  | 0   | `COMPRESSED` | Payload bytes are compressed (compression codec TBD) |
  | 1   | `SIGNED` | Stream carries a trailing HMAC or signature |
  | 2   | `INDEXED` | A seekable record index follows the stream footer |
  | 3   | `EXTENDED_CAUSALITY` | `causal_epoch` fields carry richer causal frontier data (format TBD in a future ABI revision); receivers that do not implement this MUST reject the stream rather than mis-interpret epochs as scalar clocks |
  | 4–15| Reserved | MUST be zero on write; MUST be ignored on read |

- **`feature_flags`** declares the **structural** feature sets that the parser needs
  before reading any record body — wire-layer facts only. Receivers that do not implement
  a required feature SHOULD reject the stream with a descriptive error rather than
  silently dropping ops. See the Feature level section for bit assignments.

  `feature_flags` is intentionally minimal: it covers what the transport and parser
  layers need at stream-open time (compression codec, signing, index presence, causality
  encoding). **Semantic** capability negotiation — what a session participant can do, what
  authority level they assert, what opcode extensions are semantically active — belongs in
  `SessionProfile` (opcode `0x0100`, see § Standard extensions), not here. The two layers
  are complementary and deliberately decoupled: `feature_flags` is checked once on stream
  open; `SessionProfile` is evaluated per-session by the session manager after the session
  exists and after `path_id` / `session_id` are bound.

---

## IsaHeader

Every encoded operation begins with a fixed 48-byte per-record header immediately following
the stream preamble (or the previous record). The header is organised into two processing
phases that match the session manager's read pattern: classify first (opcode + size), then
route and gate (session/partition/ordering fields). All 48 bytes fit within a single
64-byte cache line.

```c
struct IsaHeader {
    /* Phase 1 — classify (bytes 0–7, always read first) */
    uint16_t opcode;        // operation discriminant (see Opcode table)
    uint16_t flags;         // per-opcode modifier bits (0 = default)
    uint32_t length;        // total record length in bytes (header + payload)

    /* Phase 2 — route (bytes 8–31, session manager table lookups) */
    uint64_t tenant_id;     // multi-tenant scope; 0 = single-tenant / not federated
    uint64_t session_id;    // SessionTable key
    uint64_t partition_id;  // EnginePool shard key + EdgeAuthTable scope

    /* Phase 2 — gate (bytes 32–47, ordering checks, same cache line) */
    uint64_t causal_epoch;  // sender-local causal witness (semantic ordering)
    uint64_t stream_seq;    // monotone transport sequence number (gap/dup/replay)
};
// 48 bytes total; fits within a single 64-byte cache line.
```

- **`opcode`** identifies the operation. The tag `0x0000` is reserved (invalid).
- **`flags`** carry per-opcode modifiers. Undefined bits MUST be zero on write;
  receivers MUST ignore undefined bits on read (forward compatibility).
- **`length`** is the total record size including the header. A receiver that does not
  understand an opcode can skip `length − 48` payload bytes and continue parsing.
- **`tenant_id`** scopes the record to a multi-tenant or federated context. The value
  `0` means single-tenant / not federated and requires no tenant lookup. Non-zero values
  are matched against the session's allowed-tenant set at the authority gate before any
  other check. For federated replay, `tenant_id` provides the outer scoping key that
  determines which capability mask applies to the incoming record.
- **`session_id`** is the primary `SessionTable` lookup key. Every record in a stream
  carries the session to which it belongs, allowing multiplexed streams from multiple
  sessions over a single transport connection.
- **`partition_id`** is the engine shard routing key and the scope for `EdgeAuthTable`
  lookups. The session manager routes to the correct engine shard and performs edge
  authority checks using `partition_id` without reading the record body.
- **`causal_epoch`** is a monotone sender-local ordering **witness**. It establishes
  semantic ordering within a single partition's emission stream — a receiver can determine
  that record A causally precedes record B from the same sender iff
  `A.causal_epoch < B.causal_epoch`. This is a semantic field: it encodes causal
  intent, not transport delivery order. Cross-partition interpretation is
  implementation-defined for this ABI revision and MUST NOT be assumed to imply a global
  scalar clock. The `EXTENDED_CAUSALITY` stream flag (see `IsaStreamHeader.flags`) signals
  that richer causal frontier data (encoded vector clocks, Lamport timestamps with
  partition identifiers) is present; receivers that do not implement `EXTENDED_CAUSALITY`
  MUST reject such streams rather than silently mis-interpreting epochs.
- **`stream_seq`** is a monotone transport sequence number, distinct from `causal_epoch`.
  It is **session-scoped**: it increments continuously across path migrations and
  reconnects and resets only on session termination. This is intentional — it means a
  session that migrates to a new transport path does not reset its replay protection
  floor, and the session manager can detect gaps or replays across migration boundaries.
  The session manager uses `stream_seq` for gap detection (non-contiguous values indicate
  dropped records), duplicate suppression (replayed records with seen `stream_seq`
  values), and partition-local replay ordering. `stream_seq` makes no causal claim: two
  records may arrive in `stream_seq` order but carry causal witnesses from different
  causal contexts.

---

## Session manager gate semantics

The `IsaHeader` fields divide cleanly between the **session manager** (dataplane) and
the **engine** (semantic slowpath). Only records that pass the session manager's mechanical
checks reach the engine.

### Fields read by the session manager (dataplane)

| Field | Purpose |
|-------|---------|
| `opcode` | Opcode-class routing: data ops vs. control ops vs. unknown-skip |
| `flags` | Header-level modifier bits |
| `length` | Body size for skip-forward on unknown or rejected records |
| `tenant_id` | Outer federation scope check |
| `session_id` | `SessionTable` lookup → `EngineHandle`, quotas, auth mode |
| `partition_id` | `EnginePool` shard selection; `EdgeAuthTable` scope for authority gate |
| `stream_seq` | Gap detection, duplicate suppression, partition-local replay ordering |
| `causal_epoch` | Causal scope check against `EdgeAuthTable.causal_scope_bits` |

### Fields read by the engine (slowpath only)

| Field | Purpose |
|-------|---------|
| `opcode` | ISA op dispatch |
| `causal_epoch` | Semantic ordering within propagation and stabilization |
| Body payload | Node/edge identity, values, attrs |

### Authority gate

The session manager admits a record iff all of the following hold (evaluated in order,
fail-fast):

```
allowed =
    tenant_ok          // tenant_id ∈ session.allowed_tenants (0 always passes)
    && capability_ok   // PartitionAuthTable[(partition_id, opcode_class)].capability_mask satisfied
    && causal_scope_ok // causal_epoch within PartitionAuthTable[partition_id].causal_scope_bits
    && lattice_flow_ok // lattice_class compatible with PartitionAuthTable[partition_id].lattice_class
    && quota_ok        // (session_id, partition_id, opcode_class) within admission budget
```

Records that fail any gate are handled according to `FailurePolicy` (drop, delay,
backpressure signal, or escalate) — they never reach the engine.

**Granularity limit.** The dataplane gate is partition-scoped: it keys on
`(partition_id, opcode_class)` because `edge_id` is not present in `IsaHeader`. Authority
that is finer than partition-granularity — per-edge permissions, per-`(src_partition,
tgt_partition)` domain pairs, or per-capability-mask within a partition — requires parsing
the record body to recover the relevant uid. That check is the engine's responsibility,
performed after the record is admitted through the coarse dataplane gate. The two levels
are complementary: the dataplane gate filters obviously unauthorized traffic at line rate;
the engine enforces the full per-edge `EdgeLabel` semantics on the admitted subset.

### Opcode-class admission treatment

Different opcode classes have different admission semantics under load:

| Opcode class | Opcodes | Admission treatment |
|---|---|---|
| `NodeCreate`, `EdgeConnect`, `PartitionCreate`, `SessionProfile` | 0x0001, 0x0002, 0x000C–0x000E, 0x0100 | Control-plane quota; serialized; routed off hot path |
| `SetValue` | 0x0003 | Delay, drop, or coalesce (last writer wins per node uid) under pressure |
| `Propagate` | 0x0004 | Sheddable if a newer `SetValue` for the same root has been admitted |
| `Demand` | 0x0008 | Prioritized; MUST NOT be shed while caller is blocked |
| `Stabilize` | 0x000B | Slow-lane budget; preemptable via `WorkCursor` |
| All others | — | Standard FIFO within session quota |

The session manager identifies opcode class from `opcode` alone; it does not parse the
record body for admission decisions.

### `PartitionAuthTable` population

When a `PartitionCreate` or `SetPartitionAuthority` control-plane op completes on the
engine, the session manager compiles a `PartitionAuthRow` and installs it in
`PartitionAuthTable`. This is the only moment where engine semantics cross into the
dataplane for partition-level authority. After installation, all partition-scoped gate
checks are pure table lookups keyed on `(partition_id, opcode_class)`:

```
PartitionAuthRow {
    partition_id:     u64,
    opcode_class:     u8,    // which opcode class this row covers
    capability_mask:  u64,   // permitted operations within this partition × opcode_class
    lattice_class:    u8,    // chain / meet-semilattice / incomparable
    causal_scope:     u8,    // local / session / global
}
```

Per-edge authority (`EdgeLabel.capability_bits`, `EdgeLabel.causal_scope_bits`,
`EdgeLabel.projection_mask`) is compiled into `DepMeta.label` at `SetEdgeLabel` or
`PartitionBind` time and lives entirely in the engine. It is never consulted by the
dataplane; it governs which admitted records the engine allows to propagate across a
specific dependency edge.

---

## Feature level (separate from opcode namespace)

The opcode discriminant identifies *what operation* is requested. It says nothing about
*what semantic guarantees* the sender requires. These are orthogonal axes:

- A stream may use only `BASIC` opcodes but require `AUTHORITY` semantics.
- A stream may contain `0x0100`-range extension opcodes that a `BASIC`-only receiver
  safely skips via `length`.

Feature levels are declared once in `IsaStreamHeader.feature_flags`:

| Bit | Name | Covered operations |
|-----|------|--------------------|
| 0   | `BASIC` | `NodeCreate` … `SetMode` (opcodes 0x0001–0x0009) |
| 1   | `AUTHORITY` | `PartitionCreate` … `SetExecutionPolicy` (0x000C–0x0011) |
| 2   | `EGRAPH` | `Reflect`, `Stabilize` (0x000A–0x000B) |
| 3   | `DELTA` | Projection mask / shape mask propagation (future opcodes) |
| 4–63| Reserved | MUST be zero; receivers MUST ignore on read |

---

## Opcode namespace

Opcodes are `uint16_t`. The space is partitioned by range to reduce future governance
friction. Each range has distinct review and stability expectations:

| Range | Purpose | Stability |
|-------|---------|-----------|
| `0x0001–0x00FF` | Core stable ISA | Frozen; changes require ABI version bump |
| `0x0100–0x0FFF` | Standard extensions | Stable after feature graduation |
| `0x1000–0x7FFF` | Experimental / vendor tooling | No stability guarantee; not for interchange |
| `0x8000–0xFEFF` | Reserved future use | MUST NOT be emitted; receivers skip via `length` |
| `0xFF00–0xFFFF` | Internal / debug | MUST NOT appear in production streams |

The 18 core ISA ops occupy `0x0001–0x0011`. The first standard extension,
`SessionProfile` (`0x0100`), establishes the semantic capability / trust layer. Further
standard extensions (distributed merge ops, typed-zero propagation, etc.) will be allocated
from `0x0101` upward. Vendors needing private extensions use the `0x1000–0x7FFF` range
and MUST NOT expect receivers outside their toolchain to process them.

---

## Opcode table

The table is append-only: no opcode is ever renumbered or removed.

### Extension (forward propagation) — `0x0001–0x0005`

| Opcode | Hex    | Name              | Payload summary |
|--------|--------|-------------------|-----------------|
| 1      | 0x0001 | `NodeCreate`      | uid:u64, typ_len:u16, typ:utf8, rule:u8, attrs_len:u16, attrs:AttrMap |
| 2      | 0x0002 | `EdgeConnect`     | edge_id:u64, src:u64, tgt:u64, dep_kind:u8, port_kind:u8, port_name_len:u16, port_name:utf8 |
| 3      | 0x0003 | `SetValue`        | node:u64, value:i8 (−1=Neg, 0=Zero, +1=Pos) |
| 4      | 0x0004 | `Propagate`       | node:u64 |
| 5      | 0x0005 | `Subscribe`       | source:u64, subscriber:u64 |

### Inhibition (lazy / demand) — `0x0006–0x0009`

| Opcode | Hex    | Name      | Payload summary |
|--------|--------|-----------|-----------------|
| 6      | 0x0006 | `DelNode` | id:u64 |
| 7      | 0x0007 | `DelEdge` | id:u64 |
| 8      | 0x0008 | `Demand`  | node:u64 |
| 9      | 0x0009 | `SetMode` | node:u64, mode:u8 (0=Eager, 1=Lazy, 2=Stabilizing) |

### Reflection (e-graph saturation) — `0x000A–0x000B`

| Opcode | Hex    | Name        | Payload summary |
|--------|--------|-------------|-----------------|
| 10     | 0x000A | `Reflect`   | node:u64 |
| 11     | 0x000B | `Stabilize` | count:u32, node_ids:u64[count] |

### Authority / partition — `0x000C–0x0011`

| Opcode | Hex    | Name                     | Payload summary |
|--------|--------|--------------------------|-----------------|
| 12     | 0x000C | `PartitionCreate`        | id:u64, authority_root:u64, lattice_class:u64, causal_domain_bits:u64 |
| 13     | 0x000D | `PartitionBind`          | node:u64, partition:u64 |
| 14     | 0x000E | `SetPartitionAuthority`  | partition:u64, lattice_class:u64, causal_domain_bits:u64 |
| 15     | 0x000F | `SetEdgeLabel`           | src:u64, tgt:u64, lattice_class:u64, capability_bits:u8, causal_scope_bits:u64 |
| 16     | 0x0010 | `SetStabilizationConfig` | node:u64, strategy:u8, domain:u8, max_iterations:u32, convergence_policy:u8 |
| 17     | 0x0011 | `SetExecutionPolicy`     | node:u64, queue_priority:u8, retry_policy:u8 |

### Compilation hints — `0x0012`

| Opcode | Hex    | Name            | Payload summary |
|--------|--------|-----------------|-----------------|
| 18     | 0x0012 | `RegionDeclare` | root:u64, boundary_tag:u8, boundary_payload:…, stability:u8, compile:u8 |

**`RegionDeclare` semantics.** `RegionDeclare` is a local compilation hint. It declares
a subgraph as a *compiled region* — a group whose propagation the engine may execute as
a single compiled circuit rather than as individual work items. The hint carries no
semantic effect on node values or quiescent state: two engines that apply the same
stream, one respecting the hint and one ignoring it, MUST converge to the same
observable state (§ Observable convergence).

A receiver that does not implement region compilation MUST skip the payload via `length`
(§ Skipping unknown opcodes) and continue. This op is local by default — it is an
optimization hint that a co-located sender MAY include when the receiver is known to
support compilation.

**Wire layout:**

```
root:u64             — anchor node UID; the engine roots region expansion here
boundary_tag:u8      — how to determine the member set (see below)
<boundary payload>   — varies by tag
stability:u8         — epoch tracking contract (see below)
compile:u8           — when to produce the compiled artifact (see below)
```

**`boundary_tag` values:**

| Value | Name             | Boundary payload |
|-------|------------------|-----------------|
| 0x00  | `DepClosure`     | max_depth:u32 — BFS dep-closure up to this many hops from root |
| 0x01  | `ExplicitSet`    | count:u32, node_ids:u64[count] — exact member list; order not significant |
| 0x02  | `PartitionScoped`| partition_id:u64 — all nodes bound to this partition |

**`stability:u8` values:**

| Value | Name           | Meaning |
|-------|----------------|---------|
| 0x00  | `Pinned`       | Caller guarantees no topology rewrites; the engine skips epoch tracking |
| 0x01  | `EpochTracked` | Engine invalidates the compiled artifact when any member's edges change |

**`compile:u8` values:**

| Value | Name    | Meaning |
|-------|---------|---------|
| 0x00  | `Eager` | Compile immediately when the op is processed |
| 0x01  | `Lazy`  | Compile on first invocation |
| 0x02  | `Never` | Epoch tracking only; no compiled artifact is produced |

### Standard extensions — `0x0100–0x0FFF`

These opcodes are stable after feature graduation. They appear after `IsaStreamHeader` and
before or alongside data ops. Receivers that do not implement a specific extension opcode
MUST skip it via `length` (see § Skipping unknown opcodes); they MUST NOT treat an unknown
extension opcode as a wire error unless the session's `SessionProfile` declares the op as
required.

| Opcode | Hex    | Name             | Payload summary |
|--------|--------|------------------|-----------------|
| 256    | 0x0100 | `SessionProfile` | profile_id:u32, profile_version:u16, trust_level:u8, capability_mask:u64, issuer_domain:u64, session_id:u64, generation:u64, expiry_epoch:u64, sig_len:u16, signature:bytes[sig_len] |

**`SessionProfile` semantics.** A `SessionProfile` record establishes the capability mask
and trust level for the session identified by `IsaHeader.session_id`. It is the semantic
extension layer complementary to `IsaStreamHeader.feature_flags`.

- `profile_id` namespace: `0x0001–0x00FF` core profiles, `0x0100–0xFFFE` vendor,
  `0xFFFF` ad-hoc.
- `trust_level` discriminant: `0x01` = ADVISORY (no signature required; session manager
  clips `capability_mask` to `min(claimed, parent.capabilities)`), `0x02` = ASSERTED
  (signature verification against issuer trust store required), `0x03` = ATTESTED
  (signature + per-record stream MAC required).
- `capability_mask` declares the capabilities the session asserts. For ADVISORY, this is
  intersected with the parent tenant's `capabilities` ceiling before use. For ASSERTED and
  ATTESTED, the mask is accepted as-is if the signature verifies.
- The signature (ASSERTED/ATTESTED) MUST bind over the concatenation
  `capability_mask ‖ issuer_domain ‖ session_id ‖ generation ‖ expiry_epoch`. The
  `session_id` binding prevents splice attacks (importing a valid profile from a different
  session). The signature algorithm is identified by `trust_level`: ADVISORY (`0x01`) carries
  no signature; ASSERTED (`0x02`) uses Ed25519; ATTESTED (`0x03`) uses Ed25519 + stream MAC.
- `generation` is a monotone revocation counter maintained by the issuer domain. The
  receiver MUST reject profiles with `generation < min_valid_generation` for this issuer.
- `expiry_epoch` is a `causal_epoch`-based expiry (NOT wall-clock). A session manager MUST
  reject a profile once the session's `causal_epoch` has advanced past `expiry_epoch`.
  Using `causal_epoch` rather than wall-clock time ensures distributed correctness without
  requiring clock synchronization.

**`SessionProfile` wire layout** (all little-endian; total = 49 + `sig_len` bytes):

```
offset  size  field
──────  ────  ─────────────────────────────────────────────────────────────
0       4     profile_id:u32       — profile namespace (see above)
4       2     profile_version:u16  — monotone version within the profile
6       1     trust_level:u8       — 0x01=ADVISORY, 0x02=ASSERTED, 0x03=ATTESTED
7       8     capability_mask:u64  — asserted capability bits
15      8     issuer_domain:u64    — TenantId of the issuing authority
23      8     session_id:u64       — splice protection: must match stream session_id
31      8     generation:u64       — monotone revocation counter
39      8     expiry_epoch:u64     — causal_epoch at expiry; u64::MAX = never
47      2     sig_len:u16          — byte length of the signature that follows
49      sig_len  signature         — empty for ADVISORY; Ed25519/HMAC for ASSERTED/ATTESTED
```

---

## Value semantics and compute rules

Every node holds one of three values. On the wire these are encoded as `i8`:

| Wire value | Name | Meaning |
|------------|------|---------|
| +1 | **Pos** | The signal is present; the assertion holds |
| −1 | **Neg** | Pending — not yet evaluated, or inputs have not all resolved |
|  0 | **Zero** | Conflict — both a positive and a negative assertion have been made for this signal |

### Conflict infection

If any input to a node is Zero, the node outputs Zero. This applies regardless of the
compute rule. Zero is not recoverable through computation: a conflict propagates forward
unconditionally until the source is changed by a `SetValue` op.

### Pending suspension

If any input is Neg and no input is Zero, the node does not recompute — it retains its
current value. It will be re-evaluated once all inputs carry non-Neg values.

### Rule firing

A compute rule produces a result only when all inputs are Pos (after infection and
suspension have been checked). The output depends on the rule byte from `NodeCreate`:

| Rule byte | Name         | Output when all inputs are Pos | Notes |
|-----------|--------------|-------------------------------|-------|
| 0x00 | Input        | Set by `SetValue` only | No computation; value is externally driven |
| 0x01 | Identity     | Pos | Single input; passes through unchanged |
| 0x02 | MvNeg        | Neg | Inverts Pos↔Neg. Zero input → Zero (fixed point). Neg input → suspends |
| 0x03 | MvAdd        | MV-algebra sum | Full truth table to be specified in a future revision |
| 0x04 | MvMul        | MV-algebra product | Full truth table to be specified in a future revision |
| 0x05 | MvSub        | MV-algebra difference | Full truth table to be specified in a future revision |
| 0x06 | Merge        | Pos | N-ary join; Pos when conflict is absent and at least one input is Pos |
| 0x07 | MeetAll      | Pos | Conjunction: all inputs must be Pos |
| 0x08 | JoinAny      | Pos | Disjunction: any Pos input dominates once conflict is resolved |
| 0x09 | BochvarFold  | Pos | Explicit conflict fold; same infection/suspension behavior as other rules but makes it structurally visible in the graph |
| 0x0A | PowerProduct | MV-algebra power product | Full truth table to be specified in a future revision |

**MvNeg** is the only rule that produces a Neg output from computation. It expresses
logical negation: a node that is pending when its source is positive, and positive when
its source is pending. Conflict is always fixed under negation — a Zero input yields Zero.

**MeetAll** and **JoinAny** share the same infection and suspension behavior (Zero infects,
Neg suspends) and both output Pos when all inputs are Pos. Their semantic distinction
matters for intent and future partial-evaluation modes: MeetAll expresses "all conditions
must hold"; JoinAny expresses "any positive signal dominates once the signal is
uncontested."

**BochvarFold** makes the conflict and suspension logic structurally explicit in the
graph topology rather than relying on the engine's propagation pre-filter. Its behavior
is identical to MeetAll and JoinAny under the same rules; use it when the node's purpose
is specifically to aggregate and surface conflict state.

**MvAdd, MvMul, MvSub, PowerProduct** are arithmetic extensions. Infection and suspension
still apply as the pre-filter. Full truth tables will be specified in a future revision.
Receivers MUST accept these rule bytes in `NodeCreate` payloads without error.

---

## Payload encoding rules

1. **All integers** are little-endian.
2. **Strings** (typ, port_name) are UTF-8 with a preceding `u16` length; no null terminator.
3. **Uids** are opaque `u64` — the caller allocates them; they are never interpreted
   by the wire layer. Uid 0 is reserved (invalid handle).
4. **Enums** are `u8` discriminants. Unknown discriminants are a wire error (not silently ignored).
5. **`AttrMap`** — see the Attrs encoding section below.
6. **Padding**: records are NOT padded to any alignment boundary. `length` is exact.
   Consumers MUST NOT assume alignment of payload fields.

---

## Attrs encoding

`Attrs` (the `attrs` field in `NodeCreate` and future ops) carries arbitrary key-value
metadata attached to nodes and edges. The encoding must be canonical — the same logical
attr map always produces the same bytes — to preserve replay determinism and enable
content-addressed deduplication.

### Wire format

```
AttrMap ::=
    count:   u16              // number of entries; 0 = empty map
    entry[]: AttrEntry        // exactly `count` entries, sorted by key

AttrEntry ::=
    key_len: u16
    key:     utf8[key_len]    // key bytes; no null terminator
    tag:     u8               // value type discriminant (see below)
    payload: <tag-dependent>
```

### Value type tags

| Tag | Name | Payload |
|-----|------|---------|
| 0x01 | `Bool` | value:u8 (0=false, 1=true) |
| 0x02 | `Int` | value:i64 (little-endian) |
| 0x03 | `Float` | value:f64 (IEEE 754 binary64 little-endian; NaN canonicalized — see below) |
| 0x04 | `Text` | len:u16, bytes:utf8[len] |
| 0x05 | `Bytes` | len:u32, data:u8[len] |
| 0x06 | `Ternary` | value:i8 (−1=Neg, 0=Zero, +1=Pos) |
| 0x07 | `Uid` | value:u64 |
| 0x08–0xFF | Reserved | MUST NOT be emitted; receivers MUST treat as wire error |

### Float NaN normalization

IEEE 754 defines many distinct NaN bit patterns (quiet vs. signalling, varying payloads).
Allowing arbitrary NaN patterns through the wire would break canonical hashing and
`AttrMap` equality in a way that is invisible to most callers. The policy is a
serialization standard only — it does not constrain how implementations store floats
internally:

- Writers MUST canonicalize any NaN payload to the quiet NaN bit pattern
  `0x7FF8000000000000` before encoding.
- Receivers MUST treat all NaN payloads as equivalent during `AttrMap` equality
  comparison and canonical hash computation. A receiver that encounters a non-canonical
  NaN (e.g. a signalling NaN or a NaN with a non-zero payload) SHOULD normalize it
  silently rather than treating it as a wire error.
- `−NaN` (sign bit set) is normalized to `+NaN` (`0x7FF8000000000000`); negative quiet
  NaN patterns are not a distinct value in this encoding.

This is purely a serialization rule. Arithmetic behavior of floats stored in attrs is
outside the scope of this spec.

### Canonical ordering

Entries MUST be sorted lexicographically by key bytes (unsigned byte comparison, no locale).
Duplicate keys are a wire error. This gives `AttrMap` a stable canonical form for any
given logical map, which is required for:

- **Replay determinism** — same ops on fresh engine → same bytes
- **Content hashing** — `hash(AttrMap)` is stable across runtimes
- **Partial-order semantics** — see below

### Partial order

Two `AttrMap` values `A ≤ B` (A is dominated by B) iff:

1. Every key present in `A` is also present in `B` (B is a superset of A's keys), and
2. For each shared key `k`, `A[k]` and `B[k]` have the same type tag, and
3. For each shared key `k` with a ternary-ordered type (`Ternary`, `Bool`), `A[k] ≤ B[k]`
   in the ternary chain order (Neg ≤ Zero ≤ Pos; false ≤ true).
4. For non-ternary types (`Int`, `Float`, `Text`, `Bytes`, `Uid`), equality is required
   for shared keys — there is no natural chain order on these types.

This partial order supports the two-layer model (I8): the interpretation layer can grow
monotonically (new keys, advancing ternary values) without violating identity-layer stability.

### Canonical hash

The canonical hash of an `AttrMap` is `BLAKE3(canonical_wire_bytes)` where
`canonical_wire_bytes` is the encoding defined above (sorted keys, no padding).
Implementations MUST NOT use a non-canonical ordering to compute the hash.
The hash is 32 bytes and is not part of the wire format for this revision;
it is a derived value for use in content-addressed storage and deduplication.

---

## Writing records

The payload encoding rules (§ Payload encoding rules) and attrs encoding (§ Attrs
encoding) are symmetric — they define both the read and write sides. Writer obligations:

1. **Little-endian integers.** All integer fields (`u16`, `u32`, `u64`, `i8`, `i64`,
   `f64`) are little-endian with no exceptions.
2. **Exact `length` field.** `IsaHeader.length` MUST equal `48 + payload_bytes`. Receivers
   use this field to skip unknown opcodes; an incorrect `length` corrupts stream framing
   for all subsequent records.
3. **NaN canonicalization.** Any `f64` that is NaN MUST be written as quiet NaN
   `0x7FF8000000000000`. Signalling NaN and negative NaN patterns are not valid payloads.
4. **AttrMap canonical key order.** Entries MUST be sorted lexicographically by key bytes
   (unsigned byte comparison, no locale). Duplicate keys are a wire error; deduplicate
   before encoding.
5. **Strings.** UTF-8 bytes with a `u16` byte-count prefix. No null terminator. The length
   prefix is the byte count, not the character count.
6. **Uid 0 is invalid.** MUST NOT appear in any node or edge identity field.
7. **No padding.** Payloads are exact. The byte after the last payload byte is the first
   byte of the next `IsaHeader`.
8. **Zero undefined flag bits.** `IsaHeader.flags` bits not defined for a given opcode
   MUST be zero on write. `IsaStreamHeader.flags` bits 4–15 MUST be zero.
9. **feature_flags declaration.** A stream containing opcodes in the `AUTHORITY` range
   (0x000C–0x0011) MUST set the `AUTHORITY` feature flag. A stream containing `Reflect`
   or `Stabilize` (0x000A–0x000B) MUST set the `EGRAPH` flag.

A stream that violates any writer obligation is malformed. Receivers SHOULD treat
malformed streams as unrecoverable.

---

## Observable convergence

Scheduling and interleaving of internal work items (propagation queue ordering, demand
traversal order, stabilization region expansion) may vary across implementations and
may vary across runs of the same implementation. This is permitted.

The invariant is convergence of externally observable interpretations:

> For any two compliant engines `E₁` and `E₂` that process the same `IsaStreamHeader`
> followed by the same sequence of `IsaHeader` records in the same order, the value
> and version of every node MUST be identical once both engines reach a quiescent state
> (empty propagation queue).

Concretely:

- The set `{ (uid, value, version) : uid ∈ graph }` is the observable state.
- Internal queue geometry, work item ordering, and intermediate node values during
  propagation are NOT part of the observable contract.
- `stream_seq` establishes a total order within each partition; `causal_epoch` establishes
  a partial order across partition boundaries via edge interactions. Within a single
  partition, records MUST be processed in ascending `stream_seq` order. See
  § Replay determinism for the two-level definition and conformance rules.
- Implementations that process records out of `stream_seq` order within a partition, or
  that violate the cross-partition partial order, are non-conforming unless they can
  demonstrate equivalent quiescent state.

This allows parallel runtimes and multi-threaded schedulers to speculate internally as
long as they can demonstrate the convergence postcondition. It explicitly permits
implementations to differ in max_queue_depth, demand_frontier geometry, and
intermediate materialization counts — none of these are observable.

---

## Skipping unknown opcodes

A conforming receiver that encounters an opcode it does not recognise MUST:

1. Read `length` from the header (bytes 4–7).
2. Skip `length − 48` payload bytes.
3. Continue parsing from the next record.

This guarantees that streams containing future opcodes (from a higher `abi_version`
minor increment or from the `0x0100–0x7FFF` range) remain parseable by older runtimes.
Unknown opcode handling (drop vs. log vs. buffer for later) is a policy decision left
to the receiver; the wire layer only requires safe advancement.

---

## Replay determinism

The same byte stream applied to a freshly initialised engine MUST produce the same
quiescent value state for every node. Replay correctness is defined at two granularities
that form a hierarchy: partition-local ordering is a total order; cross-partition ordering
is a partial order.

### Partition-local ordering (total order)

Within a single partition, `stream_seq` establishes a strict total order over all records
emitted for that partition. Records within a partition MUST be replayed in ascending
`stream_seq` order. A non-contiguous `stream_seq` sequence (gap) indicates dropped
records; the session manager MUST either request retransmission or treat the stream as
invalid. Duplicate `stream_seq` values (reconnect replay) MUST be suppressed by the
session manager before reaching the engine.

### Cross-partition ordering (partial order)

Partitions are the units of causal isolation. Causality across partition boundaries
arises from **edge interactions**: when a record in partition P₁ causes a value to
flow across an edge into partition P₂, that interaction establishes a causal dependency.
The cross-partition causal structure is the reflexive transitive closure of these
interaction events — a partial order, not a total order.

Two records from different partitions are **concurrent** if no causal path exists between
them (no chain of edge interactions connects them). Concurrent records may be replayed in
any order consistent with the partial order without affecting quiescent convergence. This
is the central correctness claim: the observable convergence invariant (§ Observable
convergence) holds for any conforming replay ordering.

The `causal_epoch` field is a **witness projection** of this partial order. It records
the sender's local view of causal progress, which the receiver can use to respect known
dependencies. For this ABI revision, `causal_epoch` is sufficient to reconstruct
partition-local ordering and to detect obvious cross-partition violations; it does not
fully specify the cross-partition partial order unless `EXTENDED_CAUSALITY` is set.
With `EXTENDED_CAUSALITY`, the field carries encoded frontier data (format TBD in a
future ABI revision) that makes the full cross-partition partial order reconstructable
by the receiver.

### Conforming replay

A replay is conforming iff it respects:

1. **Partition-local total order**: within each partition, records appear in ascending
   `stream_seq` order.
2. **Cross-partition partial order**: if record A in P₁ causally precedes record B in P₂
   (established by edge interaction), then A appears before B in the replay sequence.
3. **Concurrent freedom**: records with no causal relationship may appear in any order.

Any conforming replay of the same stream MUST produce the same quiescent
`{ (uid, value, version) }` set across all partitions.

### Implementation requirements

Implementations MUST:

- Source Uid values from the stream (not from a local RNG) during replay.
- Not use wall-clock time as a tiebreaker for concurrent records.
- Process records in `stream_seq` order within a single partition.
- Produce canonical `AttrMap` bytes (sorted keys, NaN-normalized floats) when writing;
  accept any valid encoding when reading.
- Not assume a total order over records from different partitions unless the
  cross-partition causal structure can be verified from available `causal_epoch` witnesses
  or `EXTENDED_CAUSALITY` frontier data.

---

## Evolution policy

- **Adding an opcode**: assign the next unused tag in the appropriate range, add a row
  to the opcode table. Set the relevant `feature_flags` bit if correct operation requires it.
- **Changing a payload**: prohibited for existing opcodes. Add a new opcode with a
  version suffix (`NodeCreate_v2 = 0x0101`) if the payload must change. The original
  opcode is marked DEPRECATED but retained.
- **Removing an opcode**: prohibited. Deprecated opcodes retain their tag forever;
  receivers SHOULD treat them as no-ops after the deprecation version.
- **Bumping `abi_version`**: only for breaking wire changes that cannot be expressed
  as new opcodes (e.g. a change to `IsaHeader` layout or `IsaStreamHeader` fields).
  Should be extremely rare given the append-only opcode design. The expansion of
  `IsaHeader` from 16 bytes (`abi_version=0x0001`) to 48 bytes (`abi_version=0x0002`)
  and the subsequent expansion of `IsaStreamHeader` from 16 bytes to 32 bytes with
  `path_id` and `session_id` fields (`abi_version=0x0003`, current revision) are
  examples of layout changes that require version bumps. Receivers MUST use
  `IsaStreamHeader.abi_version` to determine which header sizes to expect; they MUST NOT
  attempt to parse a v3 stream as v2 or v1, or vice versa.

---

## psABI (calling conventions) — NOT YET FROZEN

The C/Zig function signatures for `pgress_apply`, `pgress_drain`, and related calls are
provisional. They will be frozen once ≥2 independent consumers (e.g. Python SDK and
a Zig client) have stress-tested the boundary. Provisional signatures are in
`substrate/src/ffi.h` (not yet written).
