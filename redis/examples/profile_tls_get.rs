//! Sudo-free CPU profile of the client-side TLS `GET` read path.
//!
//! Uses `pprof` (a signal/ITIMER-based sampling profiler — no `perf_event`,
//! so it works where `perf_event_paranoid` is locked down) to sample this
//! process while it hammers a pipelined `GET` over TLS, then writes a
//! flamegraph SVG. The workload mirrors `benches/bench_tls.rs`
//! `get_pipeline/sync_tls/*` so the profile explains the benchmark.
//!
//! Build with frame pointers for good stacks, then run:
//! ```
//! RUSTFLAGS="-C force-frame-pointers=yes" \
//!   cargo run --release --example profile_tls_get \
//!   --features "tokio-rustls-comp cluster" -- <value_size> <seconds>
//! ```
//! Output: `flamegraph-tls-get-<size>.svg` in the current dir.

use std::fs::File;
use std::time::{Duration, Instant};

#[path = "../tests/support/mod.rs"]
mod support;
use support::*;

fn main() {
    let mut args = std::env::args().skip(1);
    let size: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(16);
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10);
    let pipeline: usize = 1_000;

    let tempdir = tempfile::Builder::new()
        .prefix("redis-rs-prof-tls")
        .tempdir()
        .expect("tempdir");
    let tls_files = redis_test::utils::build_keys_and_certs_for_tls(&tempdir);
    let ctx = TestContext::with_tls(tls_files, false);
    let mut con = ctx.connection();

    let key = "prof_tls_key";
    let value = vec![b'x'; size];
    redis::cmd("SET").arg(key).arg(&value).exec(&mut con).unwrap();

    let mut pipe = redis::pipe();
    for _ in 0..pipeline {
        pipe.cmd("GET").arg(key);
    }

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("start pprof");

    eprintln!("profiling sync TLS pipelined GET: size={size}B pipeline={pipeline} for {secs}s ...");
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut iters: u64 = 0;
    while Instant::now() < deadline {
        let v: Vec<Vec<u8>> = pipe.query(&mut con).unwrap();
        std::hint::black_box(&v);
        iters += 1;
    }
    let gets = iters * pipeline as u64;
    eprintln!("done: {iters} pipeline iters ({gets} GETs)");

    let report = guard.report().build().expect("build report");
    let out = format!("flamegraph-tls-get-{size}.svg");
    let file = File::create(&out).expect("create svg");
    report.flamegraph(file).expect("write flamegraph");
    eprintln!("wrote {out}");
}
