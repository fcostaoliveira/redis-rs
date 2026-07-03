//! Measure per-op latency of concurrent GETs on one multiplexed connection,
//! with TCP_NODELAY on or off — quantifies the Nagle cost of the default
//! `TcpSettings { nodelay: false }` under multiplexed concurrency.
//!
//! Runs for a fixed wall-clock duration and prints a per-second throughput
//! timeline (to show the effect is continuous over long runs), then a latency
//! percentile summary. Optionally connects over TLS with mutual (client-side)
//! certificate authentication.
//!
//! Usage:
//!   nagle_probe <redis-url> <tasks> <seconds> <nodelay: true|false> \
//!       [<ca.pem> <client.crt> <client.key>]
//!
//! With the three trailing paths the URL should use the `rediss://` scheme and
//! the connection is mTLS; without them it is plaintext TCP.

use redis::io::tcp::TcpSettings;
use redis::{AsyncCommands, IntoConnectionInfo};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let mut args = std::env::args().skip(1);
    let url = args.next().expect("url");
    let tasks: usize = args.next().expect("tasks").parse().unwrap();
    let seconds: u64 = args.next().expect("seconds").parse().unwrap();
    let nodelay: bool = args.next().expect("nodelay").parse().unwrap();
    let tls_paths = (args.next(), args.next(), args.next());

    // rustls 0.23 needs a process-wide crypto provider (same as tests/support).
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    let info = url
        .into_connection_info()?
        .set_tcp_settings(TcpSettings::default().set_nodelay(nodelay));
    let client = match tls_paths {
        (Some(ca), Some(cert), Some(key)) => redis::Client::build_with_tls(
            info,
            redis::TlsCertificates {
                client_tls: Some(redis::ClientTlsConfig {
                    client_cert: std::fs::read(cert).expect("client cert"),
                    client_key: std::fs::read(key).expect("client key"),
                }),
                root_cert: Some(std::fs::read(ca).expect("ca cert")),
            },
        )?,
        _ => redis::Client::open(info)?,
    };
    let con = client.get_multiplexed_async_connection().await?;

    // 64-byte value, small command/reply so segments stay well under one MSS.
    {
        let mut c = con.clone();
        let () = c.set("nagle:probe", vec![b'x'; 64]).await?;
        for _ in 0..1000 {
            let _: Vec<u8> = c.get("nagle:probe").await?; // warmup
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..tasks {
        let mut c = con.clone();
        let stop = stop.clone();
        let completed = completed.clone();
        handles.push(tokio::spawn(async move {
            let mut lat_us = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                let _: Vec<u8> = c.get("nagle:probe").await.unwrap();
                lat_us.push(t.elapsed().as_micros() as u64);
                completed.fetch_add(1, Ordering::Relaxed);
            }
            lat_us
        }));
    }

    // Per-second throughput timeline.
    let started = Instant::now();
    let mut prev = 0u64;
    for sec in 1..=seconds {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = completed.load(Ordering::Relaxed);
        println!(
            "sec={sec} nodelay={nodelay} tasks={tasks} ops_this_sec={}",
            now - prev
        );
        prev = now;
    }
    stop.store(true, Ordering::Relaxed);
    let wall = started.elapsed();

    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    all.sort_unstable();
    let pct = |p: f64| all[((all.len() as f64 * p) as usize).min(all.len() - 1)];
    println!(
        "SUMMARY nodelay={nodelay} tasks={tasks} ops={} | p50={}us p99={}us p999={}us p9999={}us max={}us | {:.0} ops/s",
        all.len(),
        pct(0.50),
        pct(0.99),
        pct(0.999),
        pct(0.9999),
        all[all.len() - 1],
        all.len() as f64 / wall.as_secs_f64(),
    );
    Ok(())
}
