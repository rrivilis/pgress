//! Cross-shard boundary latency benchmarks.
//!
//! Measures:
//!
//! 1. **`route_record` hot path** — single-shard session manager gate pipeline
//!    throughput (ns/op). Baseline: pure in-process gate, no serialisation.
//!
//! 2. **RemoteDep encode** — serialise an `EdgeConnect` with `DepKind::Remote`
//!    (includes causal frontier). Models the encoding half of a cross-shard
//!    boundary crossing.
//!
//! 3. **RemoteDep roundtrip** — encode + decode in series. The delta over
//!    encode-only approximates decoder overhead.
//!
//! 4. **Frontier size sweep** — encode at 0/1/2/4/8/16 frontier entries to
//!    measure serialisation scaling (O(n) causal frontier cost).
//!
//! ## Interpreting results
//!
//! - p50 = steady-state cost for well-behaved traffic.
//! - p99 = tail: allocation jitter, hash map misses.
//! - (roundtrip p99) / (route_record p50) = crossing penalty in gate-cycle units.
//!
//! ## CALM adjacency
//!
//! For monotone subgraphs (all Pos, no Zero), `route_record` is the full cost:
//! coordination-free, no barriers. `remote_dep_roundtrip` measures the price
//! of making the causal boundary *explicit* — encoding the frontier rather
//! than hiding it in an implicit consistency model.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use pgress_session::{
    SessionId, PathId, TenantId, WirePartitionId, ShardId,
    admission::ShardPressure,
    domain::{
        SessionDomain, TenantDomain, PartitionDomain,
        ShardDomain, ShardFabricAddr, SessionQuota,
    },
    session::{PathEntry, PathState, SessionEntry},
    runtime::{ParsedHeader, SessionRuntime},
    encode::{write_payload, opcode_for},
    dispatch::parse_payload,
};
use pgress_core::{
    isa::IsaOp,
    node::PortKind,
    partition::{
        AuthorityMode, CapabilityBits, LatticeClass,
        PartitionId, DepKind, RemoteDep, PayloadKind, CausalScope,
    },
    time::VectorClock,
    uid,
};

// ── Shared setup helpers ──────────────────────────────────────────────────────

fn make_runtime() -> (SessionRuntime, ParsedHeader) {
    let mut rt = SessionRuntime::new();
    let sid    = SessionId(1);
    let pid    = WirePartitionId(1000);
    let path   = PathId(42);

    rt.domains.register_tenant(TenantDomain::single_tenant());
    rt.domains.register_session(SessionDomain {
        id: sid, tenant_id: TenantId(0),
        claimed_caps: CapabilityBits::ALL,
        auth_mode:    AuthorityMode::Advisory,
        quota:        SessionQuota::default(),
    });
    rt.domains.register_partition(PartitionDomain {
        id: pid, session_id: sid,
        lattice_class: LatticeClass(1),
        causal_scope:  0,
        shard_id:      ShardId(0),
    });
    rt.sessions.create(SessionEntry {
        session_id:                   sid,
        tenant_id:                    TenantId(0),
        active_path_id:               Some(path),
        prev_path_id:                 None,
        stream_seq_floor:             0,
        auth_mode:                    AuthorityMode::Advisory,
        last_active_causal_epoch:     0,
        consecutive_quiescent_epochs: 0,
    });
    rt.paths.create(PathEntry {
        path_id:             path,
        session_id:          sid,
        last_ack_stream_seq: 0,
        state:               PathState::Active,
    });
    rt.domains.shards.insert(ShardId(0), ShardDomain {
        id: ShardId(0),
        fabric_addr: Some(ShardFabricAddr { region: 0, pod: 0, rack: 0, fabric_leaf: 0 }),
    });
    rt.update_pressure(ShardId(0), ShardPressure {
        queue_depth: 0, cursor_count: 0, budget_consumed: 0,
    });

    let header = ParsedHeader {
        opcode:       0x0003,   // SetValue
        flags:        0,
        length:       16,
        tenant_id:    TenantId(0),
        session_id:   sid,
        partition_id: pid,
        causal_epoch: 1,
        stream_seq:   1,
    };
    (rt, header)
}

fn make_edge_connect_remote(n_frontier: usize) -> IsaOp {
    let src_partition = PartitionId::new_v4();
    let mut frontier  = VectorClock::new();
    for _ in 0..n_frontier {
        frontier.set(PartitionId::new_v4(), 1);
    }
    IsaOp::EdgeConnect {
        id:  uid::fresh(),
        typ: "remote-dep".into(),
        src: uid::fresh(),
        tgt: uid::fresh(),
        dep: DepKind::Remote(RemoteDep {
            source_partition: src_partition,
            source_uid:       uid::fresh(),
            source_version:   7,
            port_kind:        PortKind::Signal,
            causal_frontier:  frontier,
            payload_kind:     PayloadKind::Definite,
            source_authority: src_partition,
            emitted_class:    LatticeClass(0b0001),
            capability:       CapabilityBits(0),
            causal_scope:     CausalScope::UNIVERSAL,
        }),
    }
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

fn bench_route_record(c: &mut Criterion) {
    let mut g = c.benchmark_group("session_manager");

    // Pure admitted path: no auth denial, no pressure, no replay
    g.bench_function("route_record_admitted", |b| {
        let (mut rt, mut header) = make_runtime();
        let mut seq = 1u64;
        b.iter(|| {
            header.stream_seq   = seq;
            header.causal_epoch = seq;
            seq += 1;
            black_box(rt.route_record(black_box(&header)))
        });
    });

    // With topology pressure update after each record (simulates post-apply call)
    g.bench_function("route_record_with_pressure_update", |b| {
        let (mut rt, mut header) = make_runtime();
        let healthy = ShardPressure { queue_depth: 0, cursor_count: 0, budget_consumed: 0 };
        let mut seq = 1u64;
        b.iter(|| {
            header.stream_seq   = seq;
            header.causal_epoch = seq;
            seq += 1;
            let out = rt.route_record(black_box(&header));
            rt.update_pressure(ShardId(0), healthy);
            black_box(out)
        });
    });

    g.finish();
}

fn bench_remote_dep_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("boundary");
    let op    = make_edge_connect_remote(2);

    g.bench_function("remote_dep_encode_frontier2", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(128);
            write_payload(black_box(&op), &mut buf);
            black_box(buf)
        });
    });

    g.finish();
}

fn bench_remote_dep_roundtrip(c: &mut Criterion) {
    let mut g  = c.benchmark_group("boundary");
    let op     = make_edge_connect_remote(2);
    let opcode = opcode_for(&op);

    g.bench_function("remote_dep_roundtrip_frontier2", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(128);
            write_payload(black_box(&op), &mut buf);
            let result = parse_payload(opcode, &buf);
            black_box(result)
        });
    });

    g.finish();
}

fn bench_frontier_size_sweep(c: &mut Criterion) {
    let mut g = c.benchmark_group("boundary/frontier_sweep");

    for &n in &[0usize, 1, 2, 4, 8, 16] {
        let op = make_edge_connect_remote(n);
        g.bench_with_input(
            BenchmarkId::from_parameter(n),
            &n,
            |b, _| {
                b.iter(|| {
                    let mut buf = Vec::with_capacity(256);
                    write_payload(black_box(&op), &mut buf);
                    black_box(buf)
                });
            },
        );
    }

    g.finish();
}

criterion_group!(
    benches,
    bench_route_record,
    bench_remote_dep_encode,
    bench_remote_dep_roundtrip,
    bench_frontier_size_sweep,
);
criterion_main!(benches);
