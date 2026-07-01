//! Client-side GET microbenchmarks over TLS vs. plaintext.
//!
//! Goal: isolate the *client* read path (socket read + TLS record decrypt +
//! RESP decode) for a single `GET` so we can measure the per-call overhead TLS
//! adds and spot anything worth optimizing on the redis-rs side.
//!
//! Each benchmark pre-populates a key once, then loops on `GET` only — SET/DEL
//! are kept out of the measured section so the numbers reflect the read path.
//!
//! Requires the `tokio-rustls-comp` feature (pulls in `tls-rustls` +
//! `tokio-comp`). The plaintext variants provide the A/B baseline so the TLS
//! delta is attributable to the TLS layer rather than to unrelated noise.

use criterion::{Bencher, Criterion, Throughput, criterion_group, criterion_main};
use redis::RedisError;

use support::*;

#[path = "../tests/support/mod.rs"]
mod support;

/// Value sizes (bytes) swept so we can see where TLS record framing / decrypt
/// cost starts to dominate the fixed per-call overhead.
const VALUE_SIZES: &[usize] = &[16, 256, 4096];

fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

/// Build a plaintext test context (no TLS).
fn plaintext_ctx() -> TestContext {
    TestContext::new()
}

/// Build a TLS test context backed by freshly generated certs.
///
/// The tempdir is leaked intentionally: the cert files must outlive every
/// connection the benchmark opens, and the process exits right after.
fn tls_ctx() -> TestContext {
    let tempdir = tempfile::Builder::new()
        .prefix("redis-rs-bench-tls")
        .tempdir()
        .expect("failed to create tempdir for TLS certs");
    let tls_files = redis_test::utils::build_keys_and_certs_for_tls(&tempdir);
    let ctx = TestContext::with_tls(tls_files, false);
    std::mem::forget(tempdir);
    ctx
}

// ---------------------------------------------------------------------------
// Synchronous GET
// ---------------------------------------------------------------------------

fn bench_sync_get(b: &mut Bencher, ctx: &TestContext, value: &[u8]) {
    let mut con = ctx.connection();
    let key = "bench_tls_key";
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .exec(&mut con)
        .unwrap();

    b.iter(|| {
        let v: Vec<u8> = redis::cmd("GET").arg(key).query(&mut con).unwrap();
        std::hint::black_box(v);
    });
}

// ---------------------------------------------------------------------------
// Async (multiplexed) GET
// ---------------------------------------------------------------------------

fn bench_async_get(b: &mut Bencher, ctx: &TestContext, value: &[u8]) {
    let runtime = current_thread_runtime();
    let mut con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();
    let key = "bench_tls_key";
    runtime
        .block_on(async {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .exec_async(&mut con)
                .await
        })
        .unwrap();

    b.iter(|| {
        runtime
            .block_on(async {
                let v: Vec<u8> = redis::cmd("GET").arg(key).query_async(&mut con).await?;
                std::hint::black_box(v);
                Ok::<_, RedisError>(())
            })
            .unwrap();
    });
}

// ---------------------------------------------------------------------------
// Pipelined GET throughput
//
// A single blocking GET is dominated by the network round-trip, so TLS vs.
// plaintext is lost in RTT noise. Pipelining N GETs in one round-trip amortizes
// the RTT and lets the *client-side* per-reply cost — TLS record decrypt + RESP
// parse + Value construction — dominate, which is what we actually want to
// profile and optimize.
// ---------------------------------------------------------------------------

const PIPELINE_GETS: usize = 1_000;

fn bench_sync_pipeline_get(b: &mut Bencher, ctx: &TestContext, value: &[u8]) {
    let mut con = ctx.connection();
    let key = "bench_tls_key";
    redis::cmd("SET")
        .arg(key)
        .arg(value)
        .exec(&mut con)
        .unwrap();

    let mut pipe = redis::pipe();
    for _ in 0..PIPELINE_GETS {
        pipe.cmd("GET").arg(key);
    }

    b.iter(|| {
        let v: Vec<Vec<u8>> = pipe.query(&mut con).unwrap();
        std::hint::black_box(v);
    });
}

fn bench_async_pipeline_get(b: &mut Bencher, ctx: &TestContext, value: &[u8]) {
    let runtime = current_thread_runtime();
    let mut con = runtime
        .block_on(ctx.multiplexed_async_connection_tokio())
        .unwrap();
    let key = "bench_tls_key";
    runtime
        .block_on(async {
            redis::cmd("SET")
                .arg(key)
                .arg(value)
                .exec_async(&mut con)
                .await
        })
        .unwrap();

    let mut pipe = redis::pipe();
    for _ in 0..PIPELINE_GETS {
        pipe.cmd("GET").arg(key);
    }

    b.iter(|| {
        runtime
            .block_on(async {
                let v: Vec<Vec<u8>> = pipe.query_async(&mut con).await?;
                std::hint::black_box(v);
                Ok::<_, RedisError>(())
            })
            .unwrap();
    });
}

fn bench_get(c: &mut Criterion) {
    // Set up both servers once and reuse across the whole size sweep so the
    // server/TLS-handshake cost is not folded into the per-iteration numbers.
    let plaintext = plaintext_ctx();
    let tls = tls_ctx();

    let mut group = c.benchmark_group("get_sync");
    for &size in VALUE_SIZES {
        let value = make_value(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("plaintext/{size}"), |b| {
            bench_sync_get(b, &plaintext, &value)
        });
        group.bench_function(format!("tls/{size}"), |b| bench_sync_get(b, &tls, &value));
    }
    group.finish();

    let mut group = c.benchmark_group("get_async");
    for &size in VALUE_SIZES {
        let value = make_value(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("plaintext/{size}"), |b| {
            bench_async_get(b, &plaintext, &value)
        });
        group.bench_function(format!("tls/{size}"), |b| bench_async_get(b, &tls, &value));
    }
    group.finish();

    // Pipelined throughput: PIPELINE_GETS GETs per iteration, so the reported
    // per-iteration time divided by PIPELINE_GETS is the amortized client cost
    // per GET reply. Throughput is set in elements (GETs) for a per-op read.
    let mut group = c.benchmark_group("get_pipeline");
    group.throughput(Throughput::Elements(PIPELINE_GETS as u64));
    for &size in VALUE_SIZES {
        let value = make_value(size);
        group.bench_function(format!("sync_plaintext/{size}"), |b| {
            bench_sync_pipeline_get(b, &plaintext, &value)
        });
        group.bench_function(format!("sync_tls/{size}"), |b| {
            bench_sync_pipeline_get(b, &tls, &value)
        });
        group.bench_function(format!("async_plaintext/{size}"), |b| {
            bench_async_pipeline_get(b, &plaintext, &value)
        });
        group.bench_function(format!("async_tls/{size}"), |b| {
            bench_async_pipeline_get(b, &tls, &value)
        });
    }
    group.finish();
}

criterion_group!(bench, bench_get);
criterion_main!(bench);
