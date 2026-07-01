//! Sudo-free CPU profile of the client-side TLS pipelined `GET` over the
//! **async multiplexed** connection (the flagship path). Mirrors
//! `profile_tls_get.rs` but drives `query_async` on a current-thread runtime so
//! pprof captures the codec / fast-path / dispatch work on one thread.
//!
//! ```
//! RUSTFLAGS="-C force-frame-pointers=yes" \
//!   cargo run --release --example profile_tls_get_async \
//!   --features "tokio-rustls-comp cluster" -- <value_size> <seconds>
//! ```

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
        .prefix("redis-rs-prof-tls-async")
        .tempdir()
        .expect("tempdir");
    let tls_files = redis_test::utils::build_keys_and_certs_for_tls(&tempdir);
    let ctx = TestContext::with_tls(tls_files, false);
    let runtime = current_thread_runtime();
    let mut con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();

    let key = "prof_tls_key";
    let value = vec![b'x'; size];
    runtime
        .block_on(async {
            redis::cmd("SET")
                .arg(key)
                .arg(&value)
                .exec_async(&mut con)
                .await
        })
        .unwrap();

    let mut pipe = redis::pipe();
    for _ in 0..pipeline {
        pipe.cmd("GET").arg(key);
    }

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("start pprof");

    eprintln!("profiling ASYNC TLS pipelined GET: size={size}B pipeline={pipeline} for {secs}s ...");
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut iters: u64 = 0;
    runtime.block_on(async {
        while Instant::now() < deadline {
            let v: Vec<Vec<u8>> = pipe.query_async(&mut con).await.unwrap();
            std::hint::black_box(&v);
            iters += 1;
        }
    });
    eprintln!("done: {iters} pipeline iters ({} GETs)", iters * pipeline as u64);

    let report = guard.report().build().expect("build report");
    let out = format!("flamegraph-async-tls-get-{size}.svg");
    let file = File::create(&out).expect("create svg");
    report.flamegraph(file).expect("write flamegraph");
    eprintln!("wrote {out}");
}
